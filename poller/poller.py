import json
import time
from datetime import datetime
from typing import List, Dict, Any

# pip install curl-cffi
from curl_cffi import requests

# --- CONFIGURACIÓN ---
CATEGORY_ID = "24200"  # Portátiles
SEARCH_KEYWORDS = "portátil"
MAX_ITEMS_TO_FETCH = 100 

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


def fetch_items_with_pagination() -> List[Dict[str, Any]]:
    url = "https://api.wallapop.com/api/v3/search"
    
    params = {
        "category_id": CATEGORY_ID,
        "keywords": SEARCH_KEYWORDS,
        "source": "search_box",
        "order_by": "newest",
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
            # Un pequeño delay aleatorio ayuda a parecer más humano
            time.sleep(3)

            response = make_request(url, params)

            if response.status_code == 403:
                print(f"[!] Error 403: Bloqueado por WAF. Tu IP sigue marcada. Intenta cambiar de red (WiFi vs Datos) o espera 10 min.")
                break
            
            if response.status_code != 200:
                print(f"[!] Error API fatal ({response.status_code}): {response.text[:200]}")
                break

            try:
                data = response.json()
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
            
            all_items.extend(current_items)
            print(f"    [+] Encontrados {len(current_items)} ítems en esta página. Total acumulado: {len(all_items)}")

            next_page_token = data.get("meta", {}).get("next_page")
            
            if not next_page_token:
                print("    [-] No hay más páginas disponibles.")
                break
            
            page_count += 1

        except Exception as e:
            print(f"[!] Excepción crítica durante la petición: {e}")
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