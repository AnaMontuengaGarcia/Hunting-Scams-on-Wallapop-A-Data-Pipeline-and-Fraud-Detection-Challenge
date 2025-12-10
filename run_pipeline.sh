#!/bin/bash

# --- CONFIGURACIÓN ---
BASE_DIR="/root/wallapop-project"
VENV_DIR="$BASE_DIR/venv"

# Calculamos el nombre del archivo que generará el poller hoy
DATE_SUFFIX=$(date +%Y%m%d)
JSON_FILE="wallapop_smart_data_${DATE_SUFFIX}.json"

# Ir al directorio del proyecto (vital para que Python encuentre los módulos)
cd $BASE_DIR

# Activar el entorno virtual para usar las librerías instaladas
source $VENV_DIR/bin/activate

echo "--------------------------------------------------"
echo "[*] Iniciando Pipeline: $(date)"

# 1. EJECUTAR EL POLLER
echo "[*] Ejecutando Poller (descargando datos de Wallapop)..."
python3 poller/poller.py

# 2. VERIFICAR Y CARGAR DATOS
if [ -f "$JSON_FILE" ]; then
    echo "[OK] Archivo generado correctamente: $JSON_FILE"

    echo "[*] Ejecutando Ingesta a Elasticsearch..."
    # Le pasamos el archivo recién creado al script de ingestión
    python3 ingestion/bulk_ingest.py "$JSON_FILE"

else
    echo "[!] ERROR CRÍTICO: El poller no generó el archivo esperado ($JSON_FILE). Revisa si hubo errores arriba."
fi

echo "[*] Pipeline finalizado: $(date)"
echo "--------------------------------------------------"