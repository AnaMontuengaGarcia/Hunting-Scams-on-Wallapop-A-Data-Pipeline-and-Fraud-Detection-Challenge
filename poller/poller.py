import json
import time
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# pip install curl-cffi
from curl_cffi import requests

# --- CONFIGURACIÓN ---
# Usamos la categoría madre para dar contexto
CATEGORY_ID = "24200" 
TARGET_SUB_ID = "10310"  # El ID de portátiles (ahora como string para la URL)
MAX_ITEMS_TO_FETCH = 10000 
MAX_RETRIES = 3 # Número de intentos para peticiones de usuario

# Umbral para decidir si investigamos al usuario (hacer peticiones extra)
RISK_THRESHOLD_FOR_ENRICHMENT = 40 

SUSPICIOUS_KEYWORDS = [
    "urgente", "rotos", "para piezas", "bloqueado", "bios", 
    "sin cargador", "icloud", "reparar", "tarada", "solo hoy", 
    "sin factura", "indivisible", "no funciona", "leer bien"
]

def make_request(url: str, params: dict = None) -> requests.Response:
    """
    Realiza peticiones HTTP imitando a Chrome 110.
    Soporta params opcionales para endpoints que no son de búsqueda.
    """
    
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

    # print(f"    [DEBUG] Petición a {url}...")
    
    return requests.get(
        url, 
        params=params, 
        headers=headers, 
        impersonate="chrome110", 
        timeout=20
    )

def get_user_details(user_id: str) -> Dict[str, Any]:
    """
    Consulta el endpoint de usuario para obtener fecha de registro y reportes.
    Ref: pkg/models/profile.go
    Incluye lógica de reintentos.
    """
    url = f"https://api.wallapop.com/api/v3/users/{user_id}"
    
    for attempt in range(MAX_RETRIES):
        try:
            # Pausa pequeña para no saturar al hacer peticiones secuenciales
            time.sleep(random.uniform(1, 3)) 
            response = make_request(url)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                # Si el usuario no existe, no reintentamos
                return {}
            
            # Si es otro error (500, timeout simulado por firewall, etc), seguimos al siguiente intento
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    [!] Error conexión usuario {user_id} (Intento {attempt+1}/{MAX_RETRIES}): {e}. Reintentando...")
                time.sleep(2)
            else:
                print(f"    [!] Error fatal obteniendo detalles del usuario {user_id}: {e}")
    
    return {}

def get_user_reviews(user_id: str) -> List[Dict[str, Any]]:
    """
    Consulta las reviews del usuario.
    Ref: pkg/api/endpoints.go
    Incluye lógica de reintentos.
    """
    url = f"https://api.wallapop.com/api/v3/users/{user_id}/reviews"
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(random.uniform(1, 3))
            response = make_request(url)
            
            if response.status_code == 200:
                # La API suele devolver una lista directa o un objeto con lista
                data = response.json()
                if isinstance(data, list):
                    return data
                return data.get("reviews", [])
            elif response.status_code == 404:
                return []
                
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"    [!] Error conexión reviews {user_id} (Intento {attempt+1}/{MAX_RETRIES}): {e}. Reintentando...")
                time.sleep(2)
            else:
                print(f"    [!] Error fatal obteniendo reviews del usuario {user_id}: {e}")
    
    return []

def is_real_laptop(item: Dict[str, Any]) -> bool:
    """
    Devuelve True solo si el ítem tiene el ID 10310 en su taxonomía.
    Mantenemos esta función como doble verificación de seguridad.
    """
    taxonomy = item.get("taxonomy", [])
    for category in taxonomy:
        if str(category.get("id")) == str(TARGET_SUB_ID):
            return True
    return False

def calculate_initial_risk(item: Dict[str, Any]) -> int:
    """
    Cálculo rápido de riesgo basado SOLO en el ítem (sin llamar a APIs externas).
    Se usa para decidir si vale la pena investigar más a fondo.
    """
    score = 0
    
    # 1. Análisis de Precio
    price_obj = item.get("price", {})
    price = 0
    if isinstance(price_obj, dict):
        price = float(price_obj.get("amount", 0))
    elif isinstance(price_obj, (int, float)):
        price = float(price_obj)

    if 0 < price < 50:
        score += 30
    
    # 2. Keywords sospechosas
    title = item.get("title", "").lower()
    description = item.get("description", "").lower()
    for kw in SUSPICIOUS_KEYWORDS:
        if kw in title or kw in description:
            score += 20
            break # Solo sumamos una vez por keywords para no inflar demasiado
            
    # 3. Descripción corta
    if len(description) < 20:
        score += 10

    # 4. Flags del Ítem (La mejora que viste en el repo Go)
    # Si Wallapop ya lo ha marcado como baneado u onhold, es muy sospechoso.
    flags = item.get("flags", {})
    if flags.get("banned", False):
        score += 100 # Riesgo máximo inmediato
    if flags.get("onhold", False): # A veces "onhold" es revisión de seguridad
        score += 20
        
    return score

def enrich_and_finalize_risk(item: Dict[str, Any], initial_score: int) -> Dict[str, Any]:
    """
    Realiza la investigación profunda (Deep Dive) si el riesgo inicial es alto.
    Llama a endpoints de usuario y recalcula el score final.
    """
    factors = []
    final_score = initial_score
    
    # Recuperamos flags para documentarlos
    flags = item.get("flags", {})
    if flags.get("banned"):
        factors.append("Item BANNED by Wallapop")
    if flags.get("onhold"):
        factors.append("Item ON HOLD")

    # Si el riesgo inicial es bajo, no gastamos peticiones API extra
    if initial_score < RISK_THRESHOLD_FOR_ENRICHMENT and not flags.get("banned"):
        return {
            "risk_score": final_score,
            "risk_factors": ["Low risk check - No deep enrichment"],
            "analyzed_at": datetime.now().isoformat()
        }

    # --- INVESTIGACIÓN PROFUNDA ---
    user_id = item.get("user", {}).get("id") or item.get("user_id")
    
    if user_id:
        print(f"    [Inspect] Investigando usuario {user_id} para ítem sospechoso...")
        
        # 1. Datos del Perfil
        user_profile = get_user_details(user_id)
        
        # Fecha de registro
        register_date_ts = user_profile.get("register_date") # Suele ser timestamp millis
        if register_date_ts:
            try:
                # Convertir timestamp (ms) a datetime
                reg_date = datetime.fromtimestamp(register_date_ts / 1000)
                days_since_reg = (datetime.now() - reg_date).days
                
                if days_since_reg < 2:
                    final_score += 40
                    factors.append("User registered < 48 hours ago")
                elif days_since_reg < 7:
                    final_score += 20
                    factors.append("User registered < 1 week ago")
            except:
                pass
        
        # Reportes de estafa (si el campo existe y es visible)
        scam_reports = user_profile.get("scam_reports", 0)
        if scam_reports > 0:
            final_score += 50
            factors.append(f"User has {scam_reports} scam reports")

        # Verificación
        if not user_profile.get("verification_level", 0):
             final_score += 5
             # No es determinante, pero suma.

        # 2. Reviews del Usuario
        reviews = get_user_reviews(user_id)
        if len(reviews) == 0:
            # Vendedor sin reviews vendiendo algo "raro" o barato
            final_score += 15
            factors.append("No reviews")
        else:
            # Calcular media de estrellas
            total_stars = sum([r.get("scoring", 0) for r in reviews])
            avg_stars = total_stars / len(reviews)
            if avg_stars < 3:
                final_score += 30
                factors.append(f"Bad reputation ({avg_stars:.1f} stars)")

    # Factores básicos originales
    title = item.get("title", "").lower()
    for kw in SUSPICIOUS_KEYWORDS:
        if kw in title or item.get("description", "").lower().find(kw) != -1:
            factors.append(f"Keyword found: {kw}")
            break
            
    price_obj = item.get("price", {})
    price = 0
    if isinstance(price_obj, dict):
        price = float(price_obj.get("amount", 0))
    elif isinstance(price_obj, (int, float)):
        price = float(price_obj)
        
    if 0 < price < 50:
        factors.append("Suspiciously Low Price")

    final_score = min(final_score, 100)

    return {
        "risk_score": final_score,
        "suspicious_keywords": [kw for kw in SUSPICIOUS_KEYWORDS if kw in title],
        "risk_factors": factors,
        "user_metadata": {
            "checked": True,
            "register_date_ts": user_profile.get("register_date") if 'user_profile' in locals() else None
        },
        "analyzed_at": datetime.now().isoformat()
    }

def fetch_items_with_pagination() -> List[Dict[str, Any]]:
    url = "https://api.wallapop.com/api/v3/search"
    
    # ESTRATEGIA DE FILTRADO v7: REPLICACIÓN EXACTA DEL NAVEGADOR
    # Usamos exactamente los parámetros que has encontrado en la URL del navegador.
    # subcategory_ids (10310) + source (side_bar_filters)
    params = {
        "category_id": CATEGORY_ID,        # 24200
        "subcategory_ids": TARGET_SUB_ID,  # 10310 (El parámetro clave que nos faltaba)
        "source": "side_bar_filters",      # Indica que venimos del filtro lateral
        
        "time_filter": "today",           
        "country_code": "ES",             
        "order_by": "newest",
        "latitude": "40.4168",
        "longitude": "-3.7038",
    }

    all_items = []
    next_page_token = None
    page_count = 1
    
    print(f"[*] Iniciando búsqueda v7 (Browser Params Replica) - Objetivo: {MAX_ITEMS_TO_FETCH}...")

    try:
        while len(all_items) < MAX_ITEMS_TO_FETCH:
            if next_page_token:
                params["next_page"] = next_page_token

            try:
                print(f"    -> Solicitando página {page_count}...")
                sleep_time = random.uniform(5, 12)
                time.sleep(sleep_time)

                response = make_request(url, params)

                if response.status_code == 403:
                    print(f"[!] Error 403: WAF activado. Esperando...")
                    break
                
                if response.status_code != 200:
                    print(f"[!] Error API ({response.status_code}): {response.text[:100]}")
                    break

                data = response.json()
                
                current_items = []
                if "data" in data and "section" in data["data"]:
                     current_items = data["data"]["section"]["payload"].get("items", [])
                elif "search_objects" in data:
                    current_items = data.get("search_objects", [])
                elif "items" in data:
                    current_items = data.get("items", [])

                if not current_items:
                    print("    [ALERTA] Página vacía o fin de resultados.")
                    break

                items_of_interest = []
                discarded_count = 0
                
                for item in current_items:
                    # Mantenemos el filtro local como seguridad, pero ahora la API
                    # debería enviarnos solo portátiles.
                    if is_real_laptop(item):
                        initial_risk = calculate_initial_risk(item)
                        enrichment_data = enrich_and_finalize_risk(item, initial_risk)
                        
                        item["enrichment"] = enrichment_data
                        
                        if enrichment_data["risk_score"] >= 50:
                            print(f"        [!] FRAUDE POTENCIAL: {item.get('title')[:30]}... (Score: {enrichment_data['risk_score']})")

                        items_of_interest.append(item)
                    else:
                        discarded_count += 1
                        if discarded_count == 1:
                           # Debug para verificar si se cuela algo raro
                           print(f"        [DEBUG] Descartado: {item.get('title')[:30]}")

                all_items.extend(items_of_interest)
                print(f"    [+] Procesados {len(items_of_interest)} ítems útiles. (Descartados: {discarded_count})")

                next_page_token = data.get("meta", {}).get("next_page")
                if not next_page_token:
                    break
                
                page_count += 1

            except Exception as e:
                print(f"[!] Excepción en paginación: {e}")
                time.sleep(60) 
                continue

    except KeyboardInterrupt:
        print("\n\n[!!!] Interrupción por usuario (Ctrl+C) detectada.")
        print("[*] Deteniendo captura y procediendo a guardar los datos obtenidos...")

    return all_items[:MAX_ITEMS_TO_FETCH]

def save_items_to_daily_file(items: List[Dict[str, Any]]):
    if not items:
        return

    today_str = datetime.now().strftime("%Y%m%d")
    filename = f"wallapop_laptops_optimized_{today_str}.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        for item in items:
            item["timestamps"] = {
                "crawl_timestamp": datetime.now().isoformat()
            }
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            
    print(f"[*] ÉXITO: Guardados {len(items)} ítems en '{filename}'")

if __name__ == "__main__":
    print("--- Poller Wallapop v3: Filtrado Server-Side y Detección Avanzada ---")
    items = fetch_items_with_pagination()
    save_items_to_daily_file(items)
    print("--- Proceso finalizado ---")