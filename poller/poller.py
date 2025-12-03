import requests
import json
import time
from datetime import datetime
from typing import List, Dict, Any

# --- CONFIGURACIÓN ---
# Categoría Laptops (Según tu elección e ID del PDF)
CATEGORY_ID = "10310" 

# Búsqueda general para capturar variedad. Puedes rotar keywords si quieres.
SEARCH_KEYWORDS = "portátil laptop macbook gaming"

# Coordenadas (Madrid centro como ejemplo, requerido por la API)
LATITUDE = "40.4129297"
LONGITUDE = "-3.695283"

# Headers OBLIGATORIOS según el PDF 
HEADERS = {
    "Host": "api.wallapop.com",
    "X-DeviceOS": "0"
}

# Palabras clave sospechosas para enriquecimiento (Sección 8 del PDF)
SUSPICIOUS_KEYWORDS = [
    "urgente", "rotos", "para piezas", "bloqueado", "bios", 
    "sin cargador", "icloud", "reparar", "tarada", "solo hoy", 
    "sin factura", "indivisible", "no funciona"
]

def fetch_today_items() -> List[Dict[str, Any]]:
    """
    Consulta la API de Wallapop y devuelve la lista de ítems encontrados hoy.
    """
    url = "https://api.wallapop.com/api/v3/search"
    
    # Parámetros según documento [cite: 469]
    params = {
        "category_id": CATEGORY_ID,
        "keywords": SEARCH_KEYWORDS,
        "time_filter": "today",  # CRÍTICO: Solo ítems de hoy
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "order_by": "newest",
        "source": "search_box"
        # Opcional: "min_sale_price": "50" para filtrar basura muy barata
    }

    print(f"[*] Consultando Wallapop API para categoría {CATEGORY_ID}...")
    
    try:
        # Timeout de 10s como sugiere el ejemplo [cite: 614]
        response = requests.get(url, params=params, headers=HEADERS, timeout=10)
        response.raise_for_status() # Lanza error si no es 200 OK
        
        data = response.json()
        
        # Extracción segura de ítems navegando la estructura JSON [cite: 619]
        # La estructura suele ser: data -> search_objects (o items directos dependiendo de la versión)
        # El PDF sugiere data -> section -> payload -> items, pero la API v3 a veces varía.
        # Intentamos extraer de la lista principal de resultados:
        items = data.get("search_objects", [])
        
        if not items:
            # Intento alternativo basado en estructura antigua o variaciones
            items = data.get("data", {}).get("section", {}).get("payload", {}).get("items", [])

        print(f"[*] Se han recuperado {len(items)} ítems.")
        return items

    except requests.exceptions.RequestException as e:
        print(f"[!] Error de conexión: {e}")
        return []

def calculate_risk(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Aplica la lógica de sospecha (Sección 8) y devuelve campos de enriquecimiento.
    """
    score = 0
    factors = []
    
    # Extraer datos básicos
    price = item.get("price", {}).get("amount", 0)
    title = item.get("title", "").lower()
    description = item.get("description", "").lower()
    
    # --- REGLA 1: Palabras clave sospechosas [cite: 1324] ---
    found_keywords = []
    for kw in SUSPICIOUS_KEYWORDS:
        if kw in title or kw in description:
            found_keywords.append(kw)
    
    if found_keywords:
        score += 20 * len(found_keywords)
        factors.append(f"Keywords: {', '.join(found_keywords)}")

    # --- REGLA 2: Anomalía de precio (Simplificada) [cite: 1313] ---
    # Para Laptops, un precio funcional por debajo de 50€ suele ser sospechoso (o chatarra)
    if 0 < price < 50:
        score += 30
        factors.append("Price Very Low (<50€)")

    # --- REGLA 3: Descripción muy corta [cite: 1377] ---
    if len(description) < 20:
        score += 10
        factors.append("Short Description")

    # Normalizar score a máximo 100
    score = min(score, 100)

    # Devolvemos el objeto de enriquecimiento
    return {
        "risk_score": score,
        "suspicious_keywords": found_keywords,
        "risk_factors": factors,
        "analyzed_at": datetime.now().isoformat()
    }

def save_items_to_daily_file(items: List[Dict[str, Any]]):
    """
    Guarda los ítems en un fichero JSON Lines con fecha de hoy.
    """
    if not items:
        print("[!] No hay ítems para guardar.")
        return

    # Nombre de fichero: wallapop_laptops_YYYYMMDD.json [cite: 560]
    today_str = datetime.now().strftime("%Y%m%d")
    filename = f"wallapop_laptops_{today_str}.json"
    
    count = 0
    with open(filename, "w", encoding="utf-8") as f:
        for item in items:
            # 1. Enriquecer el ítem antes de guardar
            enrichment_data = calculate_risk(item)
            item["enrichment"] = enrichment_data
            
            # 2. Añadir timestamps útiles para Elastic [cite: 408]
            item["timestamps"] = {
                "crawl_timestamp": datetime.now().isoformat()
            }
            
            # 3. Escribir línea JSON 
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
            count += 1
            
    print(f"[*] Guardados {count} ítems enriquecidos en '{filename}'")

if __name__ == "__main__":
    # Ejecución principal
    print("--- Iniciando Poller Wallapop (Laptops) ---")
    items = fetch_today_items()
    save_items_to_daily_file(items)
    print("--- Proceso finalizado ---")