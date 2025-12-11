#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Módulo: bulk_ingest.py
Descripción: Script de ingesta masiva de documentos JSON a Elasticsearch.
             Implementa el patrón Bulk API para carga eficiente de datos.

Dependencias:
    - requests: Cliente HTTP para comunicación con Elasticsearch
    - json: Parsing y serialización de documentos JSON
    - pathlib: Manejo multiplataforma de rutas de archivos

Uso:
    python bulk_ingest.py <archivo.json>

Notas:
    - El archivo de entrada debe estar en formato NDJSON (un JSON por línea)
    - Los documentos se envían en lotes para optimizar el rendimiento
    - Compatible con Elasticsearch 7.x y 8.x
================================================================================
"""

import json
import requests
import sys
from pathlib import Path
from typing import List


# =============================================================================
# CONSTANTES DE CONFIGURACIÓN
# =============================================================================

# URL base del servidor Elasticsearch
ES_URL = "http://localhost:9200"

# Alias del índice destino donde se almacenarán los documentos
INDEX_ALIAS = "lab10310.wallapop"

# Tamaño del lote para operaciones bulk (1000 documentos es un balance
# óptimo entre rendimiento y uso de memoria)
BATCH_SIZE = 1000


# =============================================================================
# FUNCIONES DE INGESTA
# =============================================================================

def send_batch(lines: List[str]) -> None:
    """
    Envía un lote de documentos a Elasticsearch usando la Bulk API.
    
    Esta función construye el cuerpo de la petición en formato NDJSON
    (Newline Delimited JSON) y lo envía al endpoint _bulk de Elasticsearch.
    Cada documento requiere dos líneas: una de acción (index) y otra con
    los datos del documento.
    
    Args:
        lines (List[str]): Lista de líneas NDJSON alternando acciones y documentos.
                          El tamaño debe ser par (acción + documento por cada item).
    
    Returns:
        None
    
    Raises:
        No lanza excepciones, los errores se manejan internamente y se
        reportan por consola.
    
    Ejemplo:
        >>> lines = ['{"index": {"_index": "mi_indice"}}', '{"campo": "valor"}']
        >>> send_batch(lines)
        [OK] Lote de 1 docs insertado.
    
    Notas:
        - El timeout se establece en 60 segundos para lotes grandes
        - Los errores parciales de Elasticsearch se reportan pero no
          interrumpen el procesamiento
    """
    if not lines:
        return
    
    # Construir el cuerpo bulk con salto de línea final obligatorio
    bulk_body = "\n".join(lines) + "\n"
    
    try:
        response = requests.post(
            f"{ES_URL}/_bulk",
            data=bulk_body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=60  # Timeout extendido para lotes grandes
        )
        
        if response.status_code == 200:
            resp = response.json()
            if resp.get("errors"):
                # Elasticsearch procesó la petición pero algunos documentos fallaron
                print(f"    [!] Alerta: Elasticsearch reportó errores en algunos documentos del lote.")
            else:
                # Dividimos entre 2 porque cada documento tiene 2 líneas (acción + datos)
                print(f"    [OK] Lote de {len(lines)//2} docs insertado.")
        else:
            # Error HTTP (4xx, 5xx)
            print(f"    [!] Error HTTP {response.status_code}: {response.text[:200]}")
            
    except Exception as e:
        # Captura errores de red, timeout, etc.
        print(f"    [!] Excepción enviando lote: {e}")


def bulk_ingest(json_file_path: str) -> None:
    """
    Procesa un archivo JSON e ingesta su contenido en Elasticsearch.
    
    Lee un archivo en formato NDJSON línea por línea, agrupa los documentos
    en lotes del tamaño configurado y los envía a Elasticsearch usando
    la Bulk API. Este enfoque de streaming permite procesar archivos
    de cualquier tamaño sin cargarlos completamente en memoria.
    
    Args:
        json_file_path (str): Ruta al archivo JSON a procesar. Debe existir
                              y estar en formato NDJSON (un objeto JSON por línea).
    
    Returns:
        None
    
    Flujo de procesamiento:
        1. Validar existencia del archivo
        2. Leer línea por línea (streaming)
        3. Parsear cada línea como JSON
        4. Agrupar en lotes de BATCH_SIZE documentos
        5. Enviar cada lote a Elasticsearch
        6. Procesar documentos restantes al finalizar
    
    Ejemplo:
        >>> bulk_ingest("datos_wallapop_20251210.json")
        [*] Ingestando datos_wallapop_20251210.json en lotes de 1000...
        [OK] Lote de 1000 docs insertado.
        [*] Proceso finalizado. Total documentos procesados: 1500
    """
    file_path = Path(json_file_path)
    
    # Validación de existencia del archivo
    if not file_path.exists():
        print(f"[!] El archivo {json_file_path} no existe.")
        return

    print(f"[*] Ingestando {json_file_path} en lotes de {BATCH_SIZE}...")

    # Variables de control para el procesamiento por lotes
    current_batch = []  # Buffer temporal para el lote actual
    doc_count = 0       # Contador de documentos en el lote actual
    total_processed = 0 # Contador total de documentos procesados

    try:
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                
                # Ignorar líneas vacías
                if not line:
                    continue
                
                # Intentar parsear la línea como JSON
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError:
                    # Línea con JSON malformado, la saltamos
                    continue

                # Construir la estructura bulk: acción + documento
                # La acción especifica el índice destino
                action = {"index": {"_index": INDEX_ALIAS}}
                current_batch.append(json.dumps(action))
                current_batch.append(json.dumps(doc, ensure_ascii=False))
                
                doc_count += 1
                
                # Cuando alcanzamos el tamaño del lote, enviamos a Elasticsearch
                if doc_count >= BATCH_SIZE:
                    send_batch(current_batch)
                    total_processed += doc_count
                    current_batch = []  # Limpiar buffer para el siguiente lote
                    doc_count = 0

        # Enviar documentos restantes que no completaron un lote
        if current_batch:
            send_batch(current_batch)
            total_processed += doc_count
            
        print(f"[*] Proceso finalizado. Total documentos procesados: {total_processed}")

    except Exception as e:
        print(f"[!] Error inesperado leyendo archivo: {e}")


# =============================================================================
# PUNTO DE ENTRADA PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    # Validar argumentos de línea de comandos
    if len(sys.argv) > 1:
        bulk_ingest(sys.argv[1])
    else:
        print("Uso: python bulk_ingest.py <archivo.json>")