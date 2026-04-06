import asyncio
from playwright.async_api import async_playwright
import json
import re
import os
import time
import sys
from datetime import datetime, timezone
import threading
from collections import defaultdict
import nest_asyncio
from pymongo import MongoClient
from dotenv import load_dotenv
from curl_cffi import requests

# CONFIGURACIÓN DE CONSOLA PARA EMOTICONES
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

# Cargar variables de entorno
load_dotenv()

# Aplica nest_asyncio para permitir la ejecución de asyncio.run anidado.
nest_asyncio.apply()

# --- CONFIGURACIÓN MONGODB ATLAS ---
db_user = os.getenv("MONGO_USER")
db_pass = os.getenv("MONGO_PASS")
db_cluster = os.getenv("MONGO_CLUSTER")

MONGO_URI = f"mongodb+srv://{db_user}:{db_pass}@{db_cluster}/?retryWrites=true&w=majority"

try:
    client = MongoClient(MONGO_URI)
    db = client["Monitor_P2P_Bolivia"]
    collection = db["BGT_PRICE"]
    client.admin.command('ping')
except Exception as e:
    print(f"❌ Error de conexión a MongoDB: {e}")
    exit(1)

# --- URLs Definidas ---
URL_COMPRAS = "https://www.bitget.com/p2p-trade/sell?paymethodIds=-1&fiatName=BOB" 
URL_VENTAS = "https://www.bitget.com/p2p-trade?paymethodIds=-1&fiatName=BOB" 

# --- Funciones Auxiliares ---
def clean_number(text):
    if not text: return 0.0
    text = text.upper().replace("BOB", "").replace("USDT", "").replace("≈", "").replace(",", "").strip()
    match = re.findall(r"[0-9\.]+", text)
    if not match: return 0.0
    try: return float(match[0])
    except: return 0.0

def extract_limits(text):
    if not text: return 0.0, 0.0
    parts = text.replace(",", "").replace("BOB", "").replace("–", "-").split("-")
    try:
        val_min = float(re.findall(r"[0-9\.]+", parts[0])[0])
        val_max = float(re.findall(r"[0-9\.]+", parts[1])[0]) if len(parts) > 1 else val_min
        return val_min, val_max
    except:
        return 0.0, 0.0


async def scrape_bitget_p2p(url_unused: str, operation_type: str):

    api_url = "https://www.bitget.com/v1/p2p/pub/adv/queryAdvList"
    all_results = []
    prefix = f"[{operation_type.upper()}]"
    
    # Mapeo de 'side': Bitget usa 1 para COMPRA (Buy) y 2 para VENTA (Sell)
    # Según tu código original: 
    # URL_VENTAS -> compras_usdt (Side 1)
    # URL_COMPRAS -> ventas_usdt (Side 2)
    side_value = 1 if operation_type == "compras_usdt" else 2

    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://www.bitget.com",
        "referer": "https://www.bitget.com/es/p2p-trade/buy/USDT?fiatCode=BOB",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
    }

    payload = {
        "adAreaId": 0,
        "allowPlaceOrderFlag": "1",
        "attentionMerchantFlag": False,
        "coinCode": "USDT",
        "fiatCode": "BOB",
        "languageType": 7,
        "orderBy": 1,
        "pageNo": 1,
        "pageSize": 50,
        "rookieFriendlyFlag": False,
        "side": side_value
    }

    hay_mas_datos = True

    try:
        while hay_mas_datos:
            print(f"📄 {prefix} Consultando API página {payload['pageNo']}...")
            
            # Usamos impersonate para evitar detección
            response = requests.post(api_url, json=payload, headers=headers, impersonate="chrome110")
            
            if response.status_code != 200:
                print(f"❌ {prefix} Error {response.status_code} en API.")
                break

            res_json = response.json()
            data = res_json.get('data', {})
            anuncios_api = data.get('dataList', [])

            if not anuncios_api:
                break

            for adv in anuncios_api:
                # Adaptación de campos según tu requerimiento
                all_results.append({
                    "tipo": operation_type,
                    "merchant": str(adv.get('nickName', 'N/A')),
                    "precio_bob": float(adv.get('price', 0.0)),
                    "monto_usdt": float(adv.get('editAmount', 0.0)), # Como pediste: editAmount -> monto_usdt
                    "limit_min": float(adv.get('minAmount', 0.0)),
                    "limit_max": float(adv.get('maxAmount', 0.0))
                })

            hay_mas_datos = data.get('hasNextPage', False)
            if hay_mas_datos and payload['pageNo'] < 10: # Límite de seguridad
                payload['pageNo'] += 1
                time.sleep(0.5) # La API es más tolerante que la web
            else:
                hay_mas_datos = False

        print(f"📈 {prefix} Total capturado: {len(all_results)} registros.")
        return all_results

    except Exception as e:
        print(f"❌ Error en scraping API {operation_type}: {e}")
        return all_results

def procesar_datos_db(data, trade_type):
    agrupado = defaultdict(lambda: {"suma": 0.0, "conteo": 0, "min": float('inf'), "max": 0.0, "inmediato": 0.0})
    vol_total = 0.0
    
    for item in data:
        precio = item.get("precio_bob")
        cantidad_usdt = item.get("monto_usdt")
        l_min = item.get("limit_min")
        l_max = item.get("limit_max")
        
        if not precio or not cantidad_usdt: continue

        if (l_max / precio) >= cantidad_usdt:
            l_max_final = cantidad_usdt * precio
        else:
            l_max_final = l_max

        vol_total += cantidad_usdt
        agrupado[precio]["suma"] += cantidad_usdt
        agrupado[precio]["conteo"] += 1
        
        if l_min < agrupado[precio]["min"]:
            agrupado[precio]["min"] = l_min
        if l_max_final > agrupado[precio]["max"]:
            agrupado[precio]["max"] = l_max_final
        agrupado[precio]["inmediato"] += l_max_final/precio

    datos_finales = {}
    for k, v in agrupado.items():
        price_key = f"{k:.2f}".replace(".", "_")
        datos_finales[price_key] = {
            "suma": v["suma"],
            "conteo": v["conteo"],
            "min": v["min"] if v["min"] != float('inf') else 0.0,
            "max": v["max"],
            "inmediato": v["inmediato"]
        }
    
    return {
        "trade_type": trade_type,
        "vol_total": vol_total,
        "datos_agrupados": datos_finales
    }

async def main_async():
    # URL_VENTAS -> compras_usdt (porque Bitget muestra 'Vender' para quien quiere comprar)
    compras_task = scrape_bitget_p2p(URL_VENTAS, "compras_usdt")
    ventas_task = scrape_bitget_p2p(URL_COMPRAS, "ventas_usdt")
    results = await asyncio.gather(compras_task, ventas_task)
    return results[0], results[1]

def obtener_y_guardar_datos():
    print(f"\n--- 🕒 CICLO DE SCRAPING: {datetime.now().strftime('%H:%M:%S')} ---")
    
    # 1. Capturamos el tiempo de inicio
    start_time = time.time() 
    
    resultados = []
    try:
        # Ejecución del scraping
        data_compras, data_ventas = asyncio.run(main_async())
        
        # 2. Capturamos el tiempo de fin y calculamos la diferencia
        end_time = time.time()
        duration = end_time - start_time
        
        resultados.append(procesar_datos_db(data_compras, "BUY"))
        resultados.append(procesar_datos_db(data_ventas, "SELL"))
        
        documento = {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "bitget",
            "resultados": resultados
        }
        
        collection.insert_one(documento)
        
        # 3. Mostramos el tiempo transcurrido con 2 decimales
        print(f"⏱️ Tiempo de ejecución: {duration:.2f} segundos")
        print(f"✅ ÉXITO: {len(data_compras) + len(data_ventas)} anuncios guardados en MongoDB.")
        
    except Exception as e:
        print(f"❌ Error fatal en proceso principal: {e}")

def worker():
    while True:
        obtener_y_guardar_datos()
        print(f"⏳ Esperando 15 segundos para la siguiente ronda...")
        time.sleep(10)

if __name__ == '__main__':
    print("🚀 Recolector Bitget P2P -> MongoDB iniciado.")
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo recolector Bitget...")