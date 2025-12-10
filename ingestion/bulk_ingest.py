import json
import requests
import sys
from pathlib import Path
from typing import List

# --- CONFIGURACIÓN ---
ES_URL = "http://localhost:9200"
INDEX_ALIAS = "lab10310.wallapop" 
BATCH_SIZE = 1000  # Número de documentos por lote (1000 es seguro y rápido)

def send_batch(lines: List[str]):
    """Envía un lote de líneas NDJSON a Elasticsearch."""
    if not lines: return
    
    bulk_body = "\n".join(lines) + "\n"
    
    try:
        response = requests.post(
            f"{ES_URL}/_bulk",
            data=bulk_body.encode("utf-8"),
            headers={"Content-Type": "application/x-ndjson"},
            timeout=60 # Aumentamos timeout por seguridad
        )
        
        if response.status_code == 200:
            resp = response.json()
            if resp.get("errors"):
                print(f"    [!] Alerta: Elasticsearch reportó errores en algunos documentos del lote.")
                # Opcional: Loguear errores específicos
            else:
                print(f"    [OK] Lote de {len(lines)//2} docs insertado.")
        else:
            print(f"    [!] Error HTTP {response.status_code}: {response.text[:200]}")
            
    except Exception as e:
        print(f"    [!] Excepción enviando lote: {e}")

def bulk_ingest(json_file_path: str):
    file_path = Path(json_file_path)
    if not file_path.exists():
        print(f"[!] El archivo {json_file_path} no existe.")
        return

    print(f"[*] Ingestando {json_file_path} en lotes de {BATCH_SIZE}...")

    current_batch = []
    doc_count = 0
    total_processed = 0

    try:
        with file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                
                try:
                    doc = json.loads(line)
                except json.JSONDecodeError:
                    continue

                # Acción + Documento
                action = {"index": {"_index": INDEX_ALIAS}}
                current_batch.append(json.dumps(action))
                current_batch.append(json.dumps(doc, ensure_ascii=False))
                
                doc_count += 1
                
                # Si alcanzamos el tamaño del lote, enviamos
                if doc_count >= BATCH_SIZE:
                    send_batch(current_batch)
                    total_processed += doc_count
                    current_batch = [] # Limpiamos buffer
                    doc_count = 0

        # Enviar el resto final si queda algo
        if current_batch:
            send_batch(current_batch)
            total_processed += doc_count
            
        print(f"[*] Proceso finalizado. Total documentos procesados: {total_processed}")

    except Exception as e:
        print(f"[!] Error inesperado leyendo archivo: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        bulk_ingest(sys.argv[1])
    else:
        print("Uso: python bulk_ingest.py <archivo.json>")