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
MAX_ITEMS_LIMIT = 50000   # Reducido de 50k a 5k porque ahora es más lento (deep fetch)
DAYS_TO_RETRIEVE = 30    
SAVE_INTERVAL_MINUTES = 10 
MAX_RETRIES = 3

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
        "X-DeviceOS": "0",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    for attempt in range(MAX_RETRIES):
        if interrupted: return None
        try:
            # Backoff exponencial con ruido
            wait = (1.5 ** attempt) + random.uniform(0.1, 0.5)
            if attempt > 0: 
                time.sleep(wait)
            
            response = requests.get(url, params=params, headers=headers, timeout=15)
            
            if response.status_code == 403 or response.status_code == 429:
                print(f"    [PAUSA] Rate Limit/Block ({response.status_code}). Esperando 60s...")
                time.sleep(60)
                continue
                
            if response.status_code >= 500:
                continue 
                
            return response
            
        except requests.RequestException as e:
            print(f"    [NET] Error red: {e}")
            continue
            
    return None

def get_item_details_full(item_id: str) -> Dict[str, Any]:
    """Solicita los detalles completos para obtener el estado real."""
    url = f"https://api.wallapop.com/api/v3/items/{item_id}"
    # Pausa obligatoria entre detalles para evitar ban
    time.sleep(random.uniform(0, 0.01)) 
    
    resp = make_request(url)
    if resp and resp.status_code == 200:
        return resp.json()
    return {}

def save_checkpoint(items: List[Dict[str, Any]], filename: str):
    temp_filename = filename + ".tmp"
    try:
        with open(temp_filename, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
        if os.path.exists(filename): os.replace(temp_filename, filename)
        else: os.rename(temp_filename, filename)
        print(f"    [DISK] Checkpoint guardado: {len(items)} ítems.")
    except Exception as e:
        print(f"    [!] Error guardando checkpoint: {e}")

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
    cutoff_date = datetime.now() - timedelta(days=DAYS_TO_RETRIEVE)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename = f"wallapop_raw_full_{timestamp}.json"
    
    print(f"[*] Iniciando Recolección Profunda (Deep Fetch). Meta: {MAX_ITEMS_LIMIT} items.")
    print(f"    Archivo: {filename}")
    
    last_save = datetime.now()
    stop_fetching = False
    
    while len(all_items) < MAX_ITEMS_LIMIT and not stop_fetching and not interrupted:
        if next_page: params["next_page"] = next_page
        
        try:
            # Pausa entre páginas de búsqueda
            time.sleep(random.uniform(0, 0.5))
            resp = make_request(url, params)
            if not resp or resp.status_code != 200: break
                
            data = resp.json()
            items_batch = []
            if "data" in data and "section" in data["data"]:
                items_batch = data["data"]["section"]["payload"].get("items", [])
            elif "items" in data:
                items_batch = data.get("items", [])
            
            if not items_batch: break
                
            print(f"    [PÁGINA] Procesando lote de {len(items_batch)} items...")
            
            added_in_batch = 0
            for item in items_batch:
                if interrupted: break
                
                # Check fecha
                ts = item.get("creation_date")
                if ts:
                    if datetime.fromtimestamp(ts / 1000) < cutoff_date:
                        stop_fetching = True
                        break
                
                # --- DEEP FETCH (Detalles Completos) ---
                item_id = item.get("id")
                if item_id:
                    details = get_item_details_full(item_id)
                    
                    if details:
                        # 1. ESTADO DEL PRODUCTO (Crítico para Stats)
                        item["type_attributes"] = details.get("type_attributes")
                        item["is_refurbished"] = details.get("is_refurbished")
                        item["taxonomy"] = details.get("taxonomy") 
                        
                        # 2. MÉTRICAS DE DEMANDA (Crítico para Fraude)
                        # views, favorites, conversations
                        item["counters"] = details.get("counters") 
                        
                        # 3. DATOS DE ENVÍO Y RESERVA
                        item["shipping"] = details.get("shipping")
                        item["reserved"] = details.get("reserved")
                        
                        # 4. DESCRIPCIÓN COMPLETA
                        # La búsqueda a veces la corta. Si el detalle la tiene entera, la preferimos.
                        # Suele venir en description -> original
                        full_desc = details.get("description", {}).get("original")
                        if full_desc and len(full_desc) > len(item.get("description", "")):
                            item["description"] = full_desc

                all_items.append(item)
                added_in_batch += 1
                
                # Feedback visual tipo barra de progreso simple
                if len(all_items) % 10 == 0:
                    sys.stdout.write(f"\r    -> Total: {len(all_items)}/{MAX_ITEMS_LIMIT}")
                    sys.stdout.flush()
            
            print("") # Nueva línea tras el batch
            
            if (datetime.now() - last_save).total_seconds() > (SAVE_INTERVAL_MINUTES * 60):
                save_checkpoint(all_items, filename)
                last_save = datetime.now()
            
            if not stop_fetching:
                next_page = data.get("meta", {}).get("next_page")
                if not next_page: break
                    
        except Exception as e:
            print(f"[!] Error: {e}")
            break
            
    print(f"\n[*] Finalizando. Guardando {len(all_items)} ítems...")
    save_checkpoint(all_items, filename)
    print("[OK] Recolección completada.")

if __name__ == "__main__":
    run_collector()