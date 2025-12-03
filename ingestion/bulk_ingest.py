import json
import requests
import sys
from pathlib import Path
from typing import List

# --- CONFIGURACIÓN ---
# URL de tu Elasticsearch (normalmente localhost:9200 en laboratorios)
ES_URL = "http://localhost:9200"

# Nombre del ALIAS de escritura que definiste en la Sección 5 (Index Template + ILM)
# IMPORTANTE: Cambia 'labXXX' por tu número de grupo o usuario.
INDEX_ALIAS = "lab10310.wallapop" 

def bulk_ingest(json_file_path: str):
    """
    Lee un archivo de líneas JSON y las ingesta en Elasticsearch usando la Bulk API.
    """
    file_path = Path(json_file_path)
    if not file_path.exists():
        print(f"[!] El archivo {json_file_path} no existe.")
        return

    bulk_lines = []
    doc_count = 0

    print(f"[*] Leyendo archivo: {json_file_path}...")

    try:
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                
                # Validamos que la línea sea un JSON válido
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError:
                    print(f"[!] Línea ignorada (JSON inválido): {line[:50]}...")
                    continue

                # 1. Línea de acción (meta-data para Elasticsearch)
                # Indica a qué índice (alias) va el documento.
                action = {"index": {"_index": INDEX_ALIAS}}
                bulk_lines.append(json.dumps(action))

                # 2. Línea de documento (el contenido real)
                bulk_lines.append(json.dumps(doc, ensure_ascii=False))
                
                doc_count += 1

        if not bulk_lines:
            print("[!] No se encontraron documentos válidos para ingestar.")
            return

        # Preparamos el cuerpo de la petición (NDJSON debe terminar en nueva línea)
        bulk_body = "\n".join(bulk_lines) + "\n"

        print(f"[*] Enviando {doc_count} documentos a {ES_URL}/{INDEX_ALIAS}/_bulk ...")

        # Petición POST a la API _bulk
        response = requests.post(
            f"{ES_URL}/_bulk",
            data=bulk_body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=30
        )

        # Verificación de la respuesta
        if response.status_code == 200:
            resp_json = response.json()
            took = resp_json.get("took", 0)
            errors = resp_json.get("errors", False)
            
            print(f"[*] Estado: {response.status_code} OK")
            print(f"[*] Tiempo procesado: {took}ms")
            
            if errors:
                print("[!] ATENCIÓN: Hubo errores en la inserción. Revisa la respuesta detallada.")
                # Opcional: imprimir los items con error
                # print(json.dumps(resp_json, indent=2))
            else:
                print("[OK] Ingesta completada sin errores.")
        else:
            print(f"[!] Error HTTP {response.status_code}: {response.text}")

    except Exception as e:
        print(f"[!] Error inesperado: {e}")

if __name__ == "__main__":
    # Modo de uso: python bulk_ingest.py <archivo_json>
    # Si no se pasa argumento, intenta cargar un ejemplo por defecto
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
        bulk_ingest(input_file)
    else:
        # Ejemplo por defecto (ajusta la fecha si lo ejecutas sin argumentos)
        print("Uso: python bulk_ingest.py <archivo_diario.json>")
        # bulk_ingest("wallapop_laptops_20231027.json")