import json
import time
import random
import requests
import signal
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

# --- CONFIGURACIÓN DE RECOLECCIÓN ---
CATEGORY_ID = "24200"
TARGET_SUB_ID = "10310"  # Portátiles
MAX_ITEMS_LIMIT = 50000  # Objetivo ambicioso
DAYS_TO_RETRIEVE = 30    # Días de historial
SAVE_INTERVAL_MINUTES = 10 # Guardar en disco cada 10 min
MAX_RETRIES = 5

# Control de interrupción
interrupted = False

def signal_handler(sig, frame):
    global interrupted
    print("\n\n[!!!] Interrupción recibida (Ctrl+C). Deteniendo limpiamente...")
    interrupted = True

signal.signal(signal.SIGINT, signal_handler)

def make_request(url: str, params: dict = None) -> requests.Response:
    headers = {
        "Host": "api.wallapop.com",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9",
        "Origin": "https://es.wallapop.com",
        "Referer": "https://es.wallapop.com/",
        "X-DeviceOS": "0",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    for attempt in range(MAX_RETRIES):
        if interrupted: return None
        try:
            # Backoff exponencial con ruido: 2s, 4s, 8s... + random
            wait = (2 ** attempt) + random.uniform(0, 1)
            if attempt > 0: 
                print(f"    [RETRY] Intento {attempt+1}/{MAX_RETRIES} en {wait:.1f}s...")
                time.sleep(wait)
            
            response = requests.get(url, params=params, headers=headers, timeout=30)
            
            if response.status_code == 403:
                print(f"    [BLOCK] 403 Detectado. Pausa larga defensiva (2 min)...")
                time.sleep(120)
                continue # Reintentar tras la pausa
                
            if response.status_code >= 500:
                continue # Error servidor, reintentar
                
            return response
            
        except requests.RequestException as e:
            print(f"    [NET] Error red: {e}")
            continue
            
    return None

def save_checkpoint(items: List[Dict[str, Any]], filename: str):
    """Guarda los datos de forma atómica (escribe tmp y renombra) para evitar corrupción."""
    temp_filename = filename + ".tmp"
    try:
        with open(temp_filename, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
        
        # Renombrado atómico (seguro en POSIX, atomic-ish en Windows)
        if os.path.exists(filename):
            os.replace(temp_filename, filename)
        else:
            os.rename(temp_filename, filename)
            
        print(f"    [DISK] Checkpoint guardado: {len(items)} ítems en '{filename}'")
    except Exception as e:
        print(f"    [!] Error crítico guardando checkpoint: {e}")

def run_collector():
    url = "https://api.wallapop.com/api/v3/search"
    params = {
        "category_id": CATEGORY_ID,
        "subcategory_ids": TARGET_SUB_ID,
        "source": "side_bar_filters",
        "country_code": "ES",
        "order_by": "newest",
        "latitude": "40.4168",
        "longitude": "-3.7038",
    }
    
    all_items = []
    next_page = None
    
    # Fecha límite
    cutoff_date = datetime.now() - timedelta(days=DAYS_TO_RETRIEVE)
    
    # Nombre de archivo único por sesión
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename = f"wallapop_raw_data_{timestamp}.json"
    
    print(f"[*] Iniciando Recolección Robusta ({DAYS_TO_RETRIEVE} días)")
    print(f"    Archivo: {filename}")
    
    last_save = datetime.now()
    stop_fetching = False
    
    while len(all_items) < MAX_ITEMS_LIMIT and not stop_fetching and not interrupted:
        if next_page: params["next_page"] = next_page
        
        try:
            # Pausa humana aleatoria
            time.sleep(random.uniform(4, 8))
            
            resp = make_request(url, params)
            if not resp or resp.status_code != 200:
                print("[!] Fallo definitivo en petición API. Abortando recolección.")
                break
                
            try:
                data = resp.json()
            except:
                print("[!] JSON inválido recibido.")
                continue

            # Extracción agnóstica
            items_batch = []
            if "data" in data and "section" in data["data"]:
                items_batch = data["data"]["section"]["payload"].get("items", [])
            elif "items" in data:
                items_batch = data.get("items", [])
            
            if not items_batch:
                print("[*] Fin de resultados (lista vacía).")
                break
                
            # Procesar lote
            added_in_batch = 0
            for item in items_batch:
                # Comprobar fecha
                ts = item.get("creation_date")
                if ts:
                    item_date = datetime.fromtimestamp(ts / 1000)
                    if item_date < cutoff_date:
                        print(f"    [INFO] Límite temporal alcanzado ({item_date}).")
                        stop_fetching = True
                        break
                
                all_items.append(item)
                added_in_batch += 1
            
            print(f"    -> +{added_in_batch} ítems. Total: {len(all_items)}")
            
            # Guardado periódico
            if (datetime.now() - last_save).total_seconds() > (SAVE_INTERVAL_MINUTES * 60):
                save_checkpoint(all_items, filename)
                last_save = datetime.now()
            
            # Paginación
            if not stop_fetching:
                next_page = data.get("meta", {}).get("next_page")
                if not next_page: 
                    print("[*] No hay token de siguiente página.")
                    break
                    
        except Exception as e:
            print(f"[!] Excepción en bucle: {e}")
            time.sleep(5)
            
    # Guardado final
    print(f"[*] Finalizando. Guardando {len(all_items)} ítems...")
    save_checkpoint(all_items, filename)
    print("[OK] Recolección completada.")

if __name__ == "__main__":
    run_collector()