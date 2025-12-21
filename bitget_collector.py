# import asyncio
# from playwright.async_api import async_playwright
# import json
# import re
# import os
# import time
# import sys # <--- Asegúrate de importar sys
# from datetime import datetime, timezone
# import threading
# from collections import defaultdict
# import nest_asyncio
# from pymongo import MongoClient
# from dotenv import load_dotenv

# # CONFIGURACIÓN DE CONSOLA PARA EMOTICONES
# if sys.platform.startswith('win'):
#     sys.stdout.reconfigure(encoding='utf-8')

# # Cargar variables de entorno
# load_dotenv()

# # Aplica nest_asyncio para permitir la ejecución de asyncio.run anidado.
# nest_asyncio.apply()

# # --- CONFIGURACIÓN MONGODB ATLAS ---
# db_user = os.getenv("MONGO_USER")
# db_pass = os.getenv("MONGO_PASS")
# db_cluster = os.getenv("MONGO_CLUSTER")

# MONGO_URI = f"mongodb+srv://{db_user}:{db_pass}@{db_cluster}/?retryWrites=true&w=majority"

# try:
#     client = MongoClient(MONGO_URI)
#     # Cambiamos el nombre de la DB para diferenciarla de Binance
#     db = client["Monitor_P2P_Bolivia"]
#     collection = db["BGT_PRICE"]
#     client.admin.command('ping')
# except Exception as e:
#     print(f"❌ Error de conexión a MongoDB: {e}")
#     exit(1)

# # --- URLs Definidas ---
# URL_COMPRAS = "https://www.bitget.com/p2p-trade/sell?paymethodIds=-1&fiatName=BOB" 
# URL_VENTAS = "https://www.bitget.com/p2p-trade?paymethodIds=-1&fiatName=BOB" 

# # --- Funciones Auxiliares (Sin cambios) ---
# def clean_number(text):
#     if not text: return 0.0
#     text = text.upper().replace("BOB", "").replace("USDT", "").replace("≈", "").replace(",", "").strip()
#     match = re.findall(r"[0-9\.]+", text)
#     if not match: return 0.0
#     try: return float(match[0])
#     except: return 0.0

# def extract_limits(text):
#     if not text: return 0.0, 0.0
#     parts = text.replace(",", "").replace("BOB", "").replace("–", "-").split("-")
#     try:
#         val_min = float(re.findall(r"[0-9\.]+", parts[0])[0])
#         val_max = float(re.findall(r"[0-9\.]+", parts[1])[0]) if len(parts) > 1 else val_min
#         return val_min, val_max
#     except:
#         return 0.0, 0.0

# # --- Función de Scraping ---
# async def scrape_bitget_p2p(url: str, operation_type: str):
#     all_results = []
#     MAX_PAGES = 20
#     async with async_playwright() as pw:
#         browser = await pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
#         context = await browser.new_context(
#             viewport={"width": 1920, "height": 1080},
#             user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
#         )
#         page = await context.new_page()
#         try:
#             await page.goto(url, timeout=90000)
#             await page.wait_for_timeout(4000)
#             await page.keyboard.press("Escape")
#             try: await page.locator('.bit-dialog__close').click(timeout=3000)
#             except: pass
            
#             await page.wait_for_selector(".hall-list-item", state="visible", timeout=60000)
            
#             for page_num in range(1, MAX_PAGES + 1):
#                 if page_num > 1:
#                     target_page_locator = page.get_by_text(str(page_num), exact=True)
#                     if await target_page_locator.count() == 0: break
#                     await target_page_locator.click(force=True)
#                     await page.wait_for_timeout(2000)

#                 cards = await page.query_selector_all(".hall-list-item")
#                 for card in cards:
#                     name_el = await card.query_selector(".list-item__nickname")
#                     name = await name_el.inner_text() if name_el else "N/A"
#                     price_el = await card.query_selector(".price-shower")
#                     raw_price = await price_el.inner_text() if price_el else None
#                     amount_el = await card.query_selector(".list_limit span span:first-child")
#                     raw_amount = await amount_el.inner_text() if amount_el else None
#                     limit_el = await card.query_selector(".list_limit")
#                     limit_text = await limit_el.inner_text() if limit_el else ""
#                     range_match = re.search(r"([\d,.]+\s*–\s*[\d,.]+)", limit_text)
#                     range_str = range_match.group(1) if range_match else ""
#                     v_min, v_max = extract_limits(range_str)

#                     all_results.append({
#                         "tipo": operation_type,
#                         "merchant": name.strip(),
#                         "precio_bob": clean_number(raw_price),
#                         "monto_usdt": clean_number(raw_amount),
#                         "limit_min": v_min,
#                         "limit_max": v_max
#                     })
#             return all_results
#         except Exception as e:
#             print(f"Error en {operation_type}: {e}")
#             return all_results
#         finally:
#             await browser.close()

# def procesar_datos_db(data, trade_type):
#     agrupado = defaultdict(lambda: {"suma": 0.0, "conteo": 0, "min": float('inf'), "max": 0.0, "inmediato": 0.0})
#     vol_total = 0.0
    
#     for item in data:
#         precio = item.get("precio_bob")
#         cantidad_usdt = item.get("monto_usdt")
#         l_min = item.get("limit_min")
#         l_max = item.get("limit_max")
        
#         if not precio or not cantidad_usdt: continue

#         if (l_max / precio) >= cantidad_usdt:
#             l_max_final = cantidad_usdt * precio
#         else:
#             l_max_final = l_max

#         vol_total += cantidad_usdt
#         agrupado[precio]["suma"] += cantidad_usdt
#         agrupado[precio]["conteo"] += 1
        
#         if l_min < agrupado[precio]["min"]:
#             agrupado[precio]["min"] = l_min
#         if l_max_final > agrupado[precio]["max"]:
#             agrupado[precio]["max"] = l_max_final
#         agrupado[precio]["inmediato"] += l_max_final/precio

#     # Formatear llaves para MongoDB (sin puntos)
#     datos_finales = {}
#     for k, v in agrupado.items():
#         price_key = f"{k:.2f}".replace(".", "_") # Reemplazo crítico para MongoDB
#         datos_finales[price_key] = {
#             "suma": v["suma"],
#             "conteo": v["conteo"],
#             "min": v["min"] if v["min"] != float('inf') else 0.0,
#             "max": v["max"],
#             "inmediato": v["inmediato"]
#         }
    
#     return {
#         "trade_type": trade_type,
#         "vol_total": vol_total,
#         "datos_agrupados": datos_finales
#     }

# async def main_async():
#     compras_task = scrape_bitget_p2p(URL_VENTAS, "compras_usdt")
#     ventas_task = scrape_bitget_p2p(URL_COMPRAS, "ventas_usdt")
#     results = await asyncio.gather(compras_task, ventas_task)
#     return results[0], results[1]

# def obtener_y_guardar_datos():
#     print(f"-> Iniciando scraping Bitget {datetime.now().strftime('%H:%M:%S')}...")
#     resultados = []
#     try:
#         data_compras, data_ventas = asyncio.run(main_async())
#         resultados.append(procesar_datos_db(data_compras, "BUY"))
#         resultados.append(procesar_datos_db(data_ventas, "SELL"))
        
#         # Documento para MongoDB
#         documento = {
#             "timestamp": datetime.now(timezone.utc),
#             "exchange": "bitget",
#             "resultados": resultados
#         }
        
#         # Inserción en la base de datos
#         collection.insert_one(documento)
#         print(f"✅ Datos de Bitget guardados en MongoDB Atlas.")
        
#     except Exception as e:
#         print(f"❌ Error fatal en Bitget: {e}")

# def worker():
#     while True:
#         obtener_y_guardar_datos()
#         # Tiempo de espera entre rondas de scraping (Playwright es lento, 10s es ambicioso pero posible)
#         time.sleep(60)

# if __name__ == '__main__':
#     print("🚀 Recolector Bitget P2P -> MongoDB iniciado.")
#     t = threading.Thread(target=worker, daemon=True)
#     t.start()
#     try:
#         while True: time.sleep(1)
#     except KeyboardInterrupt:
#         print("🛑 Deteniendo recolector Bitget..")

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

# CONFIGURACIÓN DE CONSOLA PARA EMOTICONES
if sys.platform.startswith('win'):
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

# --- Función de Scraping ---
async def scrape_bitget_p2p(url: str, operation_type: str):
    all_results = []
    MAX_PAGES = 20
    prefix = f"[{operation_type.upper()}]"

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True, 
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                 "--disable-dev-shm-usage"
            ]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        try:
            await page.goto(url, timeout=90000)
            await page.wait_for_timeout(4000)
            await page.keyboard.press("Escape")
            try: await page.locator('.bit-dialog__close').click(timeout=3000)
            except: pass
            
            await page.wait_for_selector(".hall-list-item", state="visible", timeout=60000)
            
            for page_num in range(1, MAX_PAGES + 1):
                print(f"📄 {prefix} Procesando página {page_num}...")
                
                if page_num > 1:
                    target_page_locator = page.get_by_text(str(page_num), exact=True)
                    if await target_page_locator.count() == 0: 
                        print(f"🏁 {prefix} No se encontraron más páginas.")
                        break
                    await target_page_locator.click(force=True)
                    # Espera para que los datos carguen
                    await page.wait_for_timeout(3000)

                cards = await page.query_selector_all(".hall-list-item")
                if not cards:
                    print(f"⚠️ {prefix} Sin anuncios detectados en página {page_num}.")
                    break

                for card in cards:
                    name_el = await card.query_selector(".list-item__nickname")
                    name = await name_el.inner_text() if name_el else "N/A"
                    
                    price_el = await card.query_selector(".price-shower")
                    raw_price = await price_el.inner_text() if price_el else None
                    
                    amount_el = await card.query_selector(".list_limit span span:first-child")
                    raw_amount = await amount_el.inner_text() if amount_el else None
                    
                    limit_el = await card.query_selector(".list_limit")
                    limit_text = await limit_el.inner_text() if limit_el else ""
                    range_match = re.search(r"([\d,.]+\s*–\s*[\d,.]+)", limit_text)
                    range_str = range_match.group(1) if range_match else ""
                    v_min, v_max = extract_limits(range_str)

                    all_results.append({
                        "tipo": operation_type,
                        "merchant": name.strip(),
                        "precio_bob": clean_number(raw_price),
                        "monto_usdt": clean_number(raw_amount),
                        "limit_min": v_min,
                        "limit_max": v_max
                    })
                
                print(f"📈 {prefix} Acumulado: {len(all_results)} registros.")

            return all_results
        except Exception as e:
            print(f"❌ Error en {operation_type}: {e}")
            return all_results
        finally:
            await browser.close()

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
    resultados = []
    try:
        data_compras, data_ventas = asyncio.run(main_async())
        
        resultados.append(procesar_datos_db(data_compras, "BUY"))
        resultados.append(procesar_datos_db(data_ventas, "SELL"))
        
        documento = {
            "timestamp": datetime.now(timezone.utc),
            "exchange": "bitget",
            "resultados": resultados
        }
        
        collection.insert_one(documento)
        print(f"✅ ÉXITO: {len(data_compras) + len(data_ventas)} anuncios guardados en MongoDB.")
        
    except Exception as e:
        print(f"❌ Error fatal en proceso principal: {e}")

def worker():
    while True:
        obtener_y_guardar_datos()
        print(f"⏳ Esperando 60 segundos para la siguiente ronda...")
        time.sleep(60)

if __name__ == '__main__':
    print("🚀 Recolector Bitget P2P -> MongoDB iniciado.")
    t = threading.Thread(target=worker, daemon=True)
    t.start()
    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\n🛑 Deteniendo recolector Bitget...")
