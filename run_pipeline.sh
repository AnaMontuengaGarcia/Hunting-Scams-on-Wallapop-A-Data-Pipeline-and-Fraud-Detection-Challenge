#!/bin/bash
# ==============================================================================
# Script: run_pipeline.sh
# Descripción: Orquestador del pipeline de recolección y carga de datos de
#              Wallapop. Ejecuta secuencialmente el poller y la ingesta a
#              Elasticsearch.
#
# Flujo de ejecución:
#   1. Activar entorno virtual de Python
#   2. Ejecutar poller.py para recolectar datos de la API de Wallapop
#   3. Verificar generación del archivo JSON
#   4. Ejecutar bulk_ingest.py para cargar datos en Elasticsearch
#   5. Limpiar archivos temporales si la ingesta fue exitosa
#
# Uso:
#   ./run_pipeline.sh
#   # O programar con cron para ejecución periódica:
#   # 0 */6 * * * /root/wallapop-project/run_pipeline.sh >> /var/log/wallapop.log 2>&1
#
# Requisitos:
#   - Python 3.x con entorno virtual configurado
#   - Elasticsearch accesible en localhost:9200
#   - Módulos Python: requests, json
#
# Códigos de salida:
#   0 - Éxito completo
#   1 - Error en la ejecución del poller o ingesta
# ==============================================================================


# ==============================================================================
# CONFIGURACIÓN DE RUTAS
# ==============================================================================

# Directorio base donde se encuentra el proyecto
BASE_DIR="/root/wallapop-project"

# Ruta al entorno virtual de Python
VENV_DIR="$BASE_DIR/venv"

# Calcular el nombre del archivo que generará el poller
# El formato incluye la fecha actual: wallapop_smart_data_YYYYMMDD.json
DATE_SUFFIX=$(date +%Y%m%d)
JSON_FILE="wallapop_smart_data_${DATE_SUFFIX}.json"


# ==============================================================================
# PREPARACIÓN DEL ENTORNO
# ==============================================================================

# Cambiar al directorio del proyecto
# Esto es necesario para que Python encuentre los módulos locales
cd $BASE_DIR

# Activar el entorno virtual de Python
# Carga las librerías instaladas (requests, etc.)
source $VENV_DIR/bin/activate


# ==============================================================================
# EJECUCIÓN DEL PIPELINE
# ==============================================================================

echo "--------------------------------------------------"
echo "[*] Iniciando Pipeline: $(date)"

# -----------------------------------------------------------------------------
# PASO 1: RECOLECCIÓN DE DATOS
# Ejecuta el poller para descargar anuncios de la API de Wallapop
# -----------------------------------------------------------------------------
echo "[*] Ejecutando Poller (descargando datos de Wallapop)..."
python3 poller/poller.py


# -----------------------------------------------------------------------------
# PASO 2: VERIFICACIÓN Y CARGA DE DATOS
# Comprueba si el poller generó el archivo esperado y lo carga en Elasticsearch
# -----------------------------------------------------------------------------
if [ -f "$JSON_FILE" ]; then
    echo "[OK] Archivo generado correctamente: $JSON_FILE"

    echo "[*] Ejecutando Ingesta a Elasticsearch..."
    # Ejecutar el script de ingesta bulk con el archivo generado
    python3 ingestion/bulk_ingest.py "$JSON_FILE"

    # Verificar el código de salida del script de ingesta
    # $? contiene el código de retorno del último comando ejecutado
    if [ $? -eq 0 ]; then
        # La ingesta fue exitosa, limpiar el archivo temporal
        echo "[CLEANUP] Borrando archivo JSON para liberar espacio..."
        rm "$JSON_FILE"
    else
        # La ingesta falló, conservar el archivo para investigación
        echo "[!] AVISO: La ingesta falló, no borro el JSON por seguridad."
    fi

else
    # El poller no generó el archivo esperado
    echo "[!] ERROR CRÍTICO: El poller no generó el archivo esperado ($JSON_FILE). Revisa si hubo errores arriba."
fi


# ==============================================================================
# FINALIZACIÓN
# ==============================================================================

echo "[*] Pipeline finalizado: $(date)"
echo "--------------------------------------------------"