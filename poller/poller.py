import json
import time
import random
import requests
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Any

try:
    import regex_analyzer
except ImportError:
    print("[!] Error: No se encuentra 'regex_analyzer.py'.")
    exit(1)

# --- CONFIGURACIÓN ---
CATEGORY_ID = "24200"
TARGET_SUB_ID = "10310"
MAX_ITEMS_TO_FETCH = 30000 
STATS_FILE = "market_stats.json"

WEIGHTS = { "cpu": 0.5, "gpu": 0.3, "ram": 0.1, "category": 0.1 }

# --- CARGA DE ESTADÍSTICAS ---
def load_market_stats():
    if not os.path.exists(STATS_FILE): return {}
    try:
        with open(STATS_FILE, 'r', encoding='utf-8') as f:
            print(f"[*] Estadísticas cargadas.")
            return json.load(f)
    except: return {}

MARKET_STATS = load_market_stats()

# --- LÓGICA DE RED ---
def make_request(url: str, params: dict = None) -> requests.Response:
    headers = {
        "Host": "api.wallapop.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "X-DeviceOS": "0"
    }
    for attempt in range(3):
        try:
            if attempt > 0: time.sleep((attempt * 2) + random.uniform(0, 1))
            response = requests.get(url, params=params, headers=headers, timeout=15)
            if response.status_code == 429: 
                time.sleep(10)
                continue
            return response
        except requests.RequestException: continue
    return None

def get_user_details(user_id: str) -> Dict[str, Any]:
    url = f"https://api.wallapop.com/api/v3/users/{user_id}"
    time.sleep(random.uniform(0, 0.001))
    response = make_request(url)
    return response.json() if response and response.status_code == 200 else {}

def get_user_reviews_stats(user_id: str) -> Dict[str, float]:
    url = f"https://api.wallapop.com/api/v3/users/{user_id}/reviews"
    time.sleep(random.uniform(0, 0.001))
    response = make_request(url)
    stats = {"count": 0, "avg_stars": 0.0}
    if response and response.status_code == 200:
        try:
            reviews = response.json()
            if isinstance(reviews, list) and reviews:
                count = len(reviews)
                total = sum(r.get("review", {}).get("scoring", 0) for r in reviews)
                stats["count"] = count
                stats["avg_stars"] = round((total / count / 100) * 5, 2)
        except: pass
    return stats

def get_item_details_full(item_id: str) -> Dict[str, Any]:
    """
    Solicitud EXTRA para obtener el estado real (condition) desde type_attributes.
    Ahora se llama para TODOS los items procesados.
    """
    url = f"https://api.wallapop.com/api/v3/items/{item_id}"
    
    # PAUSA ALEATORIA SEGURA (0.1 - 0.5s)
    time.sleep(random.uniform(0, 0.001))
    
    response = make_request(url)
    return response.json() if response and response.status_code == 200 else {}

# --- MAPEO DE CONDICIONES API -> INTERNO ---
def map_api_condition(api_val: str) -> str:
    if not api_val: return None
    api_val = api_val.lower()
    if api_val == "new": return "NEW"
    if api_val == "as_good_as_new": return "LIKE_NEW"
    if api_val in ["good", "fair"]: return "USED"
    if api_val == "has_given_it_all": return "BROKEN"
    return "USED"

# --- Wrappers Regex ---
def get_prioritized_specs_and_category(title: str, description: str):
    return regex_analyzer.get_prioritized_specs_and_category(title, description)

def get_stats_for_component(stats_node, component_type, component_name):
    # stats_node ya es el nodo específico del estado (ej. PRIME -> APPLE -> USED)
    if not component_name or not stats_node: return None
    try:
        return stats_node["components"][component_type].get(component_name)
    except KeyError:
        return None

# --- CÁLCULO DE RIESGO ---
def calculate_risk_base(item, force_condition=None):
    """
    Calcula el riesgo base. Si force_condition no es None, usa esa condición 
    (obtenida de la API) en lugar de la detectada por Regex.
    """
    score = 0
    factors = []
    
    title = item.get("title", "")
    desc = item.get("description", "")
    price = regex_analyzer.clean_price(item)
    
    specs, category, regex_condition = get_prioritized_specs_and_category(title, description=desc)
    
    # DECISIÓN FINAL DE CONDICIÓN
    # Si tenemos dato de API (force_condition), manda sobre el Regex
    condition = force_condition if force_condition else regex_condition
    
    # BÚSQUEDA DE ESTADÍSTICAS POR ESTADO
    # Estructura JSON: CATEGORY -> CONDITION -> SPECS
    cat_root = MARKET_STATS.get(category, {})
    stats_node = cat_root.get(condition, {})
    
    # Fallback lógico para stats si no hay suficientes datos para el estado exacto
    fallback_used = False
    if not stats_node:
        if condition == "NEW": 
            # Si es nuevo pero no hay stats de nuevos, comparamos con Like New o Used
            stats_node = cat_root.get("LIKE_NEW") or cat_root.get("USED")
            fallback_used = True
        elif condition == "LIKE_NEW": 
            stats_node = cat_root.get("USED")
            fallback_used = True
    
    if not stats_node: stats_node = {}

    # 1. Precio Simbólico
    if price < 5.0:
        return {
            "risk_score": 0, "risk_factors": ["Symbolic Price"],
            "market_analysis": {"detected_category": "UNCERTAIN_PRICE", "detected_condition": condition, "specs_detected": specs, "composite_z_score": 0, "estimated_market_value": 0, "components_used": []}
        }

    signals = []
    for comp in ["cpu", "gpu", "ram"]:
        c_stats = get_stats_for_component(stats_node, comp, specs.get(comp))
        if c_stats and c_stats["stdev"] > 0:
            z = (price - c_stats["mean"]) / c_stats["stdev"]
            signals.append({"z": z, "weight": WEIGHTS[comp], "ref": c_stats["mean"], "src": f"{comp}:{specs.get(comp)}"})

    if stats_node.get("stdev", 0) > 0:
        z_cat = (price - stats_node["mean"]) / stats_node["stdev"]
        signals.append({"z": z_cat, "weight": WEIGHTS["category"], "ref": stats_node["mean"], "src": category})

    final_z = 0
    est_val = 0
    if signals:
        w_z_sum = sum(s["z"] * s["weight"] for s in signals)
        w_p_sum = sum(s["ref"] * s["weight"] for s in signals)
        tot_w = sum(s["weight"] for s in signals)
        if tot_w > 0:
            final_z = w_z_sum / tot_w
            est_val = w_p_sum / tot_w
            
            # Ajuste de estimación si usamos fallback
            # Si el item es NUEVO pero estamos comparando con stats de USADO, 
            # el precio estimado debería ser mayor (+20% aprox).
            if fallback_used and condition == "NEW":
                est_val *= 1.2
                # Recalculamos Z aproximado con la nueva estimación
                # (Asumiendo misma desviación estándar por falta de datos)
                stdev_ref = stats_node.get("stdev", 100) # Default stdev si no hay
                final_z = (price - est_val) / stdev_ref

    # Factores
    if final_z < -1.5: 
        score += 30
        factors.append(f"Statistically Cheap (Z={final_z:.2f}) [{condition}]")
    if final_z < -2.5: 
        score += 40
        factors.append("EXTREME Price Anomaly")
    
    if est_val > 0 and (price / est_val) < 0.4:
        score += 20
        factors.append("Critical Price Drop (<40%)")

    # Heurística básica
    if len(desc) < 30 and price > 200: 
        score += 15
        factors.append("Short Desc")
    
    contact_pat = re.compile(r"(whatsapp|6\d{8})", re.IGNORECASE)
    if contact_pat.search(desc): 
        score += 30
        factors.append("External Contact")

    return {
        "risk_score": min(score, 100),
        "risk_factors": factors,
        "market_analysis": {
            "detected_category": category,
            "detected_condition": condition,
            "specs_detected": specs,
            "composite_z_score": round(final_z, 2),
            "estimated_market_value": round(est_val, 2),
            "components_used": [s["src"] for s in signals]
        }
    }

# --- BUCLE PRINCIPAL ---
def run_smart_poller():
    url = "https://api.wallapop.com/api/v3/search"
    params = {
        "category_id": CATEGORY_ID, 
        "subcategory_ids": TARGET_SUB_ID,
        "source": "side_bar_filters", 
        "order_by": "newest", 
        #"time_filter": "today",
        "latitude": "40.4168", 
        "longitude": "-3.7038",
    }
    
    # 1. Definimos la ventana de tiempo estricta (últimas 24h)
    cutoff_date = datetime.now() - timedelta(weeks=20)
    print("--- SMART POLLER v3 (Always Full Details & Condition-Aware Pricing) ---")
    print(f"[*] Fecha de corte (24h): {cutoff_date.isoformat()}")
    
    all_items = []
    next_page_token = None
    
    while len(all_items) < MAX_ITEMS_TO_FETCH:
        if next_page_token: params["next_page"] = next_page_token
        
        response = make_request(url, params)
        if not response: break
        
        data = response.json()
        items = []
        if "data" in data and "section" in data["data"]: 
            items = data["data"]["section"]["payload"].get("items", [])
        elif "items" in data: items = data.get("items", [])
        
        if not items: break
        
        print(f"    [+] Analizando lote de {len(items)} items...")
        
        for item in items:
            # --- FILTRO DE FECHA (Nuevo) ---
            # Wallapop da timestamp en milisegundos.
            ts = item.get("modified_date") or item.get("creation_date")
            if ts:
                try:
                    item_dt = datetime.fromtimestamp(ts / 1000)
                    if item_dt < cutoff_date:
                        # Si es más viejo de 24h, lo ignoramos y pasamos al siguiente
                        continue
                except: pass # Si falla el parseo, lo dejamos pasar por si acaso

            # 0. Saneamiento inicial
            original_price = regex_analyzer.clean_price(item)
            corrected_price = False
            if original_price < 5.0:
                real = regex_analyzer.try_extract_hidden_price(item.get("title",""), item.get("description",""))
                if real: 
                    item["price"] = {"amount": real, "currency": "EUR"}
                    corrected_price = True
            
            if regex_analyzer.clean_price(item) < 1.0 and not corrected_price: continue

            # --- NUEVO: OBTENCIÓN INCONDICIONAL DE DETALLES ---
            item_id = item.get("id")
            real_condition = None
            
            if item_id:
                # Obtenemos SIEMPRE los detalles para ver el estado real
                details = get_item_details_full(item_id)
                
                # Extraer condition de type_attributes
                api_cond_val = None
                type_attrs = details.get("type_attributes", {})
                if type_attrs and "condition" in type_attrs:
                    api_cond_val = type_attrs["condition"].get("value")
                
                real_condition = map_api_condition(api_cond_val)
                
                # También comprobamos si es refurbished
                if details.get("is_refurbished", {}).get("flag") is True:
                     real_condition = "LIKE_NEW"

                # Guardamos datos extra en el item enriquecido (útil para Kibana/Investigación)
                item["counters"] = details.get("counters") # views, favorites
                item["shipping_allowed"] = details.get("shipping", {}).get("user_allows_shipping")

            # 1. CÁLCULO RIESGO (Usando la condición real forzada y la tabla de precios correcta)
            risk_data = calculate_risk_base(item, force_condition=real_condition)
            
            if real_condition:
                 risk_data["risk_factors"].append(f"Verified Condition: {real_condition}")

            # 2. DECISIÓN DE ENRIQUECIMIENTO EXTRA (Usuarios/Reviews)
            # Aunque ya tenemos detalles del item, seguimos enriqueciendo datos de usuario 
            # solo si hay algún riesgo o interés, para no multiplicar peticiones x3.
            should_enrich_user = False
            
            if risk_data["market_analysis"].get("composite_z_score", 0) < -1.5: should_enrich_user = True
            if "External Contact" in str(risk_data["risk_factors"]): should_enrich_user = True
            if corrected_price: should_enrich_user = True

            # 3. ENRIQUECIMIENTO USUARIO
            if should_enrich_user:
                user_id = item.get("user", {}).get("id") or item.get("user_id")

                if user_id:
                    u_details = get_user_details(user_id)
                    u_stats = get_user_reviews_stats(user_id)
                    
                    # Lógica Reputación
                    sales = u_stats["count"]
                    stars = u_stats["avg_stars"]
                    badges = u_details.get("badges", [])
                    is_top = "TOP" in str(badges).upper() or u_details.get("type") == "pro"
                    
                    if sales > 5 and stars >= 4.5:
                        risk_data["risk_score"] -= 30
                        risk_data["risk_factors"].append(f"Trusted Seller ({sales}+ reviews)")
                    if is_top:
                        risk_data["risk_score"] -= 50
                        risk_data["risk_factors"].append("TOP SELLER")

                    reg_ts = u_details.get("register_date")
                    if reg_ts:
                        days = (datetime.now() - datetime.fromtimestamp(reg_ts/1000)).days
                        if days < 3: 
                            risk_data["risk_score"] += 30
                            risk_data["risk_factors"].append("New User")
                        if days > 365 and sales == 0:
                            risk_data["risk_score"] += 20
                            risk_data["risk_factors"].append("Dormant Account")
                    
                    if u_details.get("scam_reports", 0) > 0:
                        risk_data["risk_score"] = 100
                        risk_data["risk_factors"].append("REPORTED SCAMMER")

            # Clamp final
            risk_data["risk_score"] = max(0, min(100, risk_data["risk_score"]))
            item["enrichment"] = risk_data
            
            # Este bloque crea el campo "location.geo" necesario para los mapas de Elastic
            loc = item.get("location", {})
            if "latitude" in loc and "longitude" in loc:
                loc["geo"] = {"lat": loc["latitude"], "lon": loc["longitude"]}

            # --- Timestamping ---
            item["timestamps"] = {"crawl_timestamp": datetime.now().isoformat()}
            
            all_items.append(item)
            
            if risk_data["risk_score"] >= 50:
                print(f"        [!] ALERTA (Score {risk_data['risk_score']}): {item.get('title')[:40]}...")

        next_page_token = data.get("meta", {}).get("next_page")
        if not next_page_token: break
    
    # Save
    if all_items:
        fname = f"wallapop_smart_data_{datetime.now().strftime('%Y%m%d')}.json"
        with open(fname, "w", encoding="utf-8") as f:
            for i in all_items: f.write(json.dumps(i, ensure_ascii=False)+"\n")
        print(f"[OK] {len(all_items)} items guardados en {fname}")

if __name__ == "__main__":
    run_smart_poller()