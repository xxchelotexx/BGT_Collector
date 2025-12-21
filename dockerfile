# 1. IMAGEN BASE: Usar la etiqueta 'jammy' que es estable y existe.
FROM mcr.microsoft.com/playwright/python:jammy

# # 2. DIRECTORIO DE TRABAJO
# WORKDIR /app

# # 3. INSTALAR DEPENDENCIAS DE PYTHON: (Ahora instalará la versión 1.55.0 de Playwright)
# COPY requirements.txt .
# RUN pip install --no-cache-dir -r requirements.txt

# # 4. COPIAR CÓDIGO
# COPY . .

# # 5. COMANDO DE INICIO
# CMD ["python", "bitget_collector.py"]
# Directorio de trabajo
WORKDIR /app

# Copiar archivos
COPY . .

# Instalar dependencias de Python
RUN pip install --no-cache-dir -r requirements.txt

# Instalar los navegadores de Playwright (por si acaso no están en el path)
RUN playwright install chromium

# Comando para ejecutar el script
CMD ["python", "tu_nombre_de_archivo.py"]