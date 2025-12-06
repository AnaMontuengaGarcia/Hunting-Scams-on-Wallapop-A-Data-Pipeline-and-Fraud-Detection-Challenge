import json
import time
import random
from datetime import datetime
from typing import List, Dict, Any

# pip install curl-cffi
from curl_cffi import requests

# --- CONFIGURACIÓN ---
# Usamos la categoría madre para que la API no nos oculte nada
CATEGORY_ID = "24200" 
TARGET_SUB_ID = 10310  # El ID real de portátiles para filtrar nosotros
MAX_ITEMS_TO_FETCH = 10000 

SUSPICIOUS_KEYWORDS = [
    "urgente", "rotos", "para piezas", "bloqueado", "bios", 
    "sin cargador", "icloud", "reparar", "tarada", "solo hoy", 
    "sin factura", "indivisible", "no funciona"
]

def make_request(url: str, params: dict) -> requests.Response:
    """
    Estrategia Corregida: ALINEACIÓN TOTAL DE VERSIONES
    User-Agent y Huella TLS deben coincidir exactamente (Chrome 110).
    """
    
    headers = {
        "Host": "api.wallapop.com",
        "Accept": "application/json, text/plain, */*",
        # CORRECCIÓN CRÍTICA: User-Agent ajustado a Chrome 110 para coincidir con impersonate="chrome110"
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9",
        "Origin": "https://es.wallapop.com",
        "Referer": "https://es.wallapop.com/",
        "X-DeviceOS": "0", 
        "X-Requested-With": "XMLHttpRequest"
    }

    print("    [DEBUG] Enviando petición (Perfil: Desktop Chrome 110 Alineado)...")
    
    # Usamos chrome110 tanto en la huella TLS como en el User-Agent
    return requests.get(
        url, 
        params=params, 
        headers=headers, 
        impersonate="chrome110", 
        timeout=20
    )

def is_real_laptop(item: Dict[str, Any]) -> bool:
    """
    Devuelve True solo si el ítem tiene el ID 10310 en su taxonomía.
    """
    taxonomy = item.get("taxonomy", [])
    for category in taxonomy:
        if category.get("id") == TARGET_SUB_ID:
            return True
    return False

def fetch_items_with_pagination() -> List[Dict[str, Any]]:
    url = "https://api.wallapop.com/api/v3/search"
    
    params = {
        "category_id": CATEGORY_ID,
        "time_filter": "today",
        "source": "search_box",
        "order_by": "newest",
        "latitude": "40.4168",   # Madrid (o tu ciudad) [cite: 5, 11]
        "longitude": "-3.7038",  # Madrid
    }

    all_items = []
    next_page_token = None
    page_count = 1
    
    print(f"[*] Iniciando búsqueda paginada (Objetivo: {MAX_ITEMS_TO_FETCH} ítems)...")

    while len(all_items) < MAX_ITEMS_TO_FETCH:
        if next_page_token:
            params["next_page"] = next_page_token

        try:
            print(f"    -> Solicitando página {page_count}...")
            # ESTRATEGIA ANTI-BOT:
            # Esperar entre 5 y 12 segundos aleatorios.
            # Esto reduce la velocidad (evita el timeout) y parece humano.
            sleep_time = random.uniform(5, 12)
            print(f"    [DORMIR] Pausa táctica de {sleep_time:.2f} segundos...")
            time.sleep(sleep_time)

            response = make_request(url, params)

            if response.status_code == 403:
                print(f"[!] Error 403: Bloqueado por WAF. Tu IP sigue marcada. Intenta cambiar de red (WiFi vs Datos) o espera 10 min.")
                break
            
            if response.status_code != 200:
                print(f"[!] Error API fatal ({response.status_code}): {response.text[:200]}")
                break

            try:
                data = response.json()
                # --- DEBUG TEMPORAL ---
                # Si la lista de items está vacía, imprime qué devolvió la API realmente
                if "items" not in str(data): 
                    print(f"[DEBUG API] Respuesta cruda: {json.dumps(data, indent=2)}")
                # ----------------------
            except:
                print("[!] Error decodificando JSON. Respuesta no válida.")
                break
            
            if "error" in data:
                print(f"    [!] API devolvió error lógico: {data['error']}")
            
            current_items = []
            if "data" in data and "section" in data["data"]:
                 current_items = data["data"]["section"]["payload"].get("items", [])
            elif "search_objects" in data:
                current_items = data.get("search_objects", [])
            elif "items" in data:
                current_items = data.get("items", [])

            if not current_items:
                print("    [ALERTA] Página vacía (Shadowban activo).")
                print("    --- DIAGNÓSTICO ---")
                print("    Wallapop ha detectado el script. Prueba lo siguiente:")
                print("    1. Cambia la IP (Modo avión ON/OFF).")
                print("    2. Si usas tethering (compartir datos), intenta ejecutarlo conectado a otra red.")
                break


            items_of_interest = []
            for item in current_items:
                # FILTRO: Solo guardamos si es realmente un portátil (ID 10310 en taxonomía)
                if is_real_laptop(item):
                    items_of_interest.append(item)
                else:
                    # Opcional: Imprimir qué estamos descartando para depurar
                    # print(f"Descartado: {item.get('title')} (No es portátil)")
                    pass

            # Añadimos solo los buenos a la lista total
            all_items.extend(items_of_interest)
            print(f"    [+] Guardados {len(items_of_interest)} portátiles reales de {len(current_items)} descargados.")

            next_page_token = data.get("meta", {}).get("next_page")
            
            if not next_page_token:
                print("    [-] No hay más páginas disponibles.")
                break
            
            page_count += 1

        except Exception as e:
            error_msg = str(e).lower()
            # Si es un error de timeout o conexión, NO nos rendimos
            if "time out" in error_msg or "timed out" in error_msg or "connection" in error_msg:
                print(f"[!] Timeout detectado en pág {page_count}. Esperando 60s para reintentar...")
                time.sleep(60) 
                continue  # <--- Vuelve arriba e intenta la MISMA página otra vez
            
            # Si es otro error raro, entonces sí paramos
            print(f"[!] Excepción crítica no recuperable: {e}")
            break

    return all_items[:MAX_ITEMS_TO_FETCH]

def calculate_risk(item: Dict[str, Any]) -> Dict[str, Any]:
    score = 0
    factors = []
    
    price_obj = item.get("price", {})
    price = 0
    if isinstance(price_obj, dict):
        price = float(price_obj.get("amount", 0))
    elif isinstance(price_obj, (int, float)):
        price = float(price_obj)

    title = item.get("title", "").lower()
    description = item.get("description", "").lower()
    
    found_keywords = []
    for kw in SUSPICIOUS_KEYWORDS:
        if kw in title or kw in description:
            found_keywords.append(kw)
    
    if found_keywords:
        score += 20 * len(found_keywords)
        factors.append(f"Keywords: {', '.join(found_keywords)}")

    if 0 < price < 50:
        score += 30
        factors.append("Price Very Low (<50€)")

    if len(description) < 20:
        score += 10
        factors.append("Short Description")

    score = min(score, 100)

    return {
        "risk_score": score,
        "suspicious_keywords": found_keywords,
        "risk_factors": factors,
        "analyzed_at": datetime.now().isoformat()
    }

def save_items_to_daily_file(items: List[Dict[str, Any]]):
    if not items:
        print("[!] No hay ítems para guardar.")
        return

    today_str = datetime.now().strftime("%Y%m%d")
    filename = f"wallapop_laptops_{today_str}.json"
    
    count = 0
    with open(filename, "w", encoding="utf-8") as f:
        for item in items:
            enrichment_data = calculate_risk(item)
            item["enrichment"] = enrichment_data
            item["timestamps"] = {
                "crawl_timestamp": datetime.now().isoformat()
            }
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
            
    print(f"[*] ÉXITO: Guardados {count} ítems en '{filename}'")

if __name__ == "__main__":
    print("--- Iniciando Poller Wallapop (Laptops) - ESTRATEGIA CHROME 110 ALINEADA ---")
    items = fetch_items_with_pagination()
    save_items_to_daily_file(items)
    print("--- Proceso finalizado ---")