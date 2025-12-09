import json
import time
import random
import requests
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple

# IMPORTANTE: Importamos tus funciones de análisis para usarlas en tiempo real
# Asegúrate de que regex_analyzer.py esté en la misma carpeta
try:
    import regex_analyzer
except ImportError:
    print("[!] Error: No se encuentra 'regex_analyzer.py'. Asegúrate de que está en la misma carpeta.")
    exit(1)

# --- CONFIGURACIÓN ---
CATEGORY_ID = "24200"
TARGET_SUB_ID = "10310"
MAX_ITEMS_TO_FETCH = 20000 
STATS_FILE = "market_stats.json"

# Pesos de importancia para la valoración (Juicio de Experto)
WEIGHTS = {
    "cpu": 0.5,      # La CPU define la mitad del valor
    "gpu": 0.3,      # La gráfica es crítica en gaming
    "ram": 0.1,      # La RAM ajusta el precio marginalmente
    "category": 0.1  # La categoría base da el suelo del precio
}

# Límites lógicos de RAM para evitar falsos positivos
RAM_LIMITS = {
    "CHROMEBOOK": 16,
    "SURFACE": 32,
    "PREMIUM_ULTRABOOK": 64,
    "GENERICO": 64
}

# --- CARGA DE ESTADÍSTICAS ---
def load_market_stats():
    if not os.path.exists(STATS_FILE):
        print(f"[!] ADVERTENCIA: No se encontró '{STATS_FILE}'. La detección estadística no funcionará.")
        return {}
    try:
        with open(STATS_FILE, 'r', encoding='utf-8') as f:
            print(f"[*] Estadísticas de mercado cargadas desde '{STATS_FILE}'.")
            return json.load(f)
    except Exception as e:
        print(f"[!] Error leyendo stats: {e}")
        return {}

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
            # Backoff exponencial suave
            if attempt > 0: 
                sleep_time = (attempt * 2) + random.uniform(0, 1)
                time.sleep(sleep_time)
            
            response = requests.get(url, params=params, headers=headers, timeout=15)
            
            if response.status_code == 429: # Rate Limit
                time.sleep(10)
                continue
                
            return response
        except requests.RequestException:
            continue
    return None

def get_user_details(user_id: str) -> Dict[str, Any]:
    """Consulta el endpoint de usuario para obtener antigüedad y reportes."""
    url = f"https://api.wallapop.com/api/v3/users/{user_id}"
    # Pequeña pausa para no saturar al pedir detalles
    time.sleep(random.uniform(0.5, 1.5))
    
    response = make_request(url)
    if response and response.status_code == 200:
        return response.json()
    return {}

# --- FUNCIONES DE ANÁLISIS ---
def get_stats_for_component(category_node, component_type, component_name):
    if not component_name: return None
    try:
        return category_node["components"][component_type].get(component_name)
    except KeyError:
        return None

def smart_truncate_spam(text: str) -> str:
    lines = text.split('\n')
    clean_lines = []
    spam_indicators = ["rtx", "gtx", "amd", "intel", "ryzen", "i7", "i5", "ps5", "xbox", "iphone", "samsung", "asus", "msi"]
    
    for line in lines:
        hits = 0
        line_lower = line.lower()
        for ind in spam_indicators:
            if ind in line_lower: hits += 1
        if hits > 3: break
        clean_lines.append(line)
    return "\n".join(clean_lines)

def sanitize_hardware_ambiguities(text: str) -> str:
    text = re.sub(r"(?i)\b(ssd|disco|disk|drive|almacenamiento)\s+m\.?2\b", r"\1_NVME", text)
    text = re.sub(r"(?i)\bm\.?2\s+(ssd|nvme|sata)\b", r"NVME_\1", text)
    return text

def apply_category_constraints(specs: Dict, category: str, full_text_original: str) -> Dict:
    limit_ram = RAM_LIMITS.get(category, 128)
    current_ram_gb = 0
    if specs.get("ram"):
        try:
            current_ram_gb = int(re.sub(r"[^0-9]", "", specs["ram"]))
        except: pass
        
    if current_ram_gb > limit_ram:
        corrected_ram = regex_analyzer.extract_ram(full_text_original.lower(), max_gb=limit_ram)
        if corrected_ram:
            specs["ram"] = corrected_ram
        else:
            specs["ram"] = None

    if category == "CHROMEBOOK" and specs.get("cpu") and "I7" in specs["cpu"]:
        if "celeron" in full_text_original.lower():
            specs["cpu"] = "INTEL CELERON"
        elif "pentium" in full_text_original.lower():
             specs["cpu"] = "INTEL PENTIUM"
    return specs

def get_prioritized_specs_and_category(title: str, description: str) -> Tuple[Dict, str]:
    title_clean = sanitize_hardware_ambiguities(title)
    desc_clean_spam = smart_truncate_spam(description)
    desc_clean = sanitize_hardware_ambiguities(desc_clean_spam)
    
    title_lower = title_clean.lower()
    desc_truncated = desc_clean[:400] 
    
    specs_title = regex_analyzer.extract_specs_regex(title_clean)
    specs_desc = regex_analyzer.extract_specs_regex(desc_truncated)
    
    final_specs = {
        "cpu": specs_title.get("cpu") if specs_title.get("cpu") else specs_desc.get("cpu"),
        "ram": specs_title.get("ram") if specs_title.get("ram") else specs_desc.get("ram"),
        "gpu": specs_title.get("gpu") if specs_title.get("gpu") else specs_desc.get("gpu"),
    }
    
    category = "GENERICO"
    if "chromebook" in title_lower:
        category = "CHROMEBOOK"
        final_specs["gpu"] = None 
    elif any(x in title_lower for x in ["macbook", "mac air", "mac pro", "imac"]):
        category = "APPLE"
    elif "surface" in title_lower and "microsoft" in title_lower:
         category = "SURFACE"
    else:
        full_text_clean = f"{title_clean}. {desc_truncated}"
        category = regex_analyzer.classify_prime_category(full_text_clean.lower(), final_specs)
    
    full_text_for_correction = f"{title_clean} {desc_truncated}"
    final_specs = apply_category_constraints(final_specs, category, full_text_for_correction)
        
    return final_specs, category

# --- LÓGICA DE RIESGO AVANZADO ---
def calculate_advanced_risk(item: Dict[str, Any]) -> Dict[str, Any]:
    score = 0
    factors = []
    
    title = item.get("title", "")
    desc = item.get("description", "")
    price = regex_analyzer.clean_price(item)
    
    specs, category = get_prioritized_specs_and_category(title, desc)
    stats_node = MARKET_STATS.get(category, {})
    
    # 1. Ajuste de Precio Simbólico (Si no se ha corregido o sigue siendo bajo tras corrección)
    # Si el precio sigue siendo < 5€ después del saneamiento, es UNCERTAIN
    if price < 5.0:
        return {
            "risk_score": 0, # No damos riesgo alto, simplemente lo marcamos como incierto/basura
            "risk_factors": ["Price is Symbolic/Placeholder"],
            "market_analysis": {
                "detected_category": "UNCERTAIN_PRICE",
                "specs_detected": specs,
                "composite_z_score": 0,
                "estimated_market_value": 0,
                "components_used": []
            }
        }

    signals = [] 
    
    # Análisis Estadístico (Z-Score)
    for comp in ["cpu", "gpu", "ram"]:
        comp_val = specs.get(comp)
        comp_stats = get_stats_for_component(stats_node, comp, comp_val)
        if comp_stats and comp_stats["stdev"] > 0:
            z = (price - comp_stats["mean"]) / comp_stats["stdev"]
            signals.append({
                "z": z, "weight": WEIGHTS[comp], 
                "ref_price": comp_stats["mean"], "source": f"{comp.upper()}:{comp_val}"
            })

    if stats_node.get("stdev", 0) > 0:
        z_cat = (price - stats_node["mean"]) / stats_node["stdev"]
        signals.append({
            "z": z_cat, "weight": WEIGHTS["category"], 
            "ref_price": stats_node["mean"], "source": f"CAT:{category}"
        })

    final_z_score = 0
    total_weight = 0
    estimated_market_value = 0
    
    if signals:
        weighted_z_sum = sum(s["z"] * s["weight"] for s in signals)
        weighted_price_sum = sum(s["ref_price"] * s["weight"] for s in signals)
        total_weight = sum(s["weight"] for s in signals)
        
        if total_weight > 0:
            final_z_score = weighted_z_sum / total_weight
            estimated_market_value = weighted_price_sum / total_weight
    
    # Factores de Precio
    if final_z_score < -1.5:
        score += 30
        factors.append(f"Statistically Cheap (Combined Z={final_z_score:.2f})")
    if final_z_score < -2.5:
        score += 40 
        factors.append(f"EXTREME Price Anomaly (Target ~{int(estimated_market_value)}€)")

    if estimated_market_value > 0 and price > 0:
        ratio = price / estimated_market_value
        if ratio < 0.4:
            score += 20
            factors.append(f"Price is <40% of est. value ({int(ratio*100)}%)")

    # Factores Heurísticos
    contact_pattern = re.compile(r"(whatsapp|watsap|wasap|tlf|telefono|6\d{2}[\s\.]?\d{3}[\s\.]?\d{3}|gmail|hotmail)", re.IGNORECASE)
    if contact_pattern.search(desc + " " + title):
        score += 30
        factors.append("External Contact Request (WhatsApp/Phone)")

    if len(desc) < 30 and price > 200:
        score += 15
        factors.append("Very Short Description (<30 chars)")

    images = item.get("images", [])
    if len(images) <= 1 and price > 300:
        score += 15
        factors.append("Low Image Count (0-1 photos)")
    
    payment_scam_words = [
        "bizum", "transferencia", "ingreso", 
        "paypal", "pay pal", "pai pal", "pay-pal",
        "envio incluido", "pago por adelantado", "solo envio",
        "halcash", "correos prepago", "western union", "moneygram"
    ]
    found_payment = [kw for kw in payment_scam_words if kw in (title + " " + desc).lower()]
    if found_payment:
        score += 30
        factors.append(f"Risky Payment Method: {found_payment}")

    suspicious_keywords = ["urgente", "roto", "bloqueado", "bios", "icloud", "pieza", "tarada"]
    found_kws = [kw for kw in suspicious_keywords if kw in (title + " " + desc).lower()]
    if found_kws:
        score += 20
        factors.append(f"Suspicious keywords: {found_kws}")
    
    if title.isupper() and len(title) > 10:
        score += 10
        factors.append("Aggressive Title (ALL CAPS)")

    return {
        "risk_score": min(score, 100),
        "risk_factors": factors,
        "market_analysis": {
            "detected_category": category,
            "specs_detected": specs,
            "composite_z_score": round(final_z_score, 2),
            "estimated_market_value": round(estimated_market_value, 2),
            "components_used": [s["source"] for s in signals]
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
        "time_filter": "today",
        "latitude": "40.4168",
        "longitude": "-3.7038",
    }
    
    print("--- INICIANDO SMART POLLER (Intelligent Enrichment + Triggers) ---")
    print(f"[*] Objetivo: Recolectar hasta {MAX_ITEMS_TO_FETCH} ítems.")
    
    all_items = []
    next_page_token = None
    page_count = 1
    
    try:
        while len(all_items) < MAX_ITEMS_TO_FETCH:
            if next_page_token: params["next_page"] = next_page_token

            print(f"    -> Solicitando página {page_count}...")
            time.sleep(random.uniform(1, 2)) 

            response = make_request(url, params)
            if not response or response.status_code != 200:
                print(f"[!] Error API/Red. Deteniendo.")
                break

            data = response.json()
            items = []
            if "data" in data and "section" in data["data"]:
                 items = data["data"]["section"]["payload"].get("items", [])
            elif "search_objects" in data:
                 items = data.get("search_objects", [])
            elif "items" in data:
                 items = data.get("items", [])

            if not items:
                print("    [INFO] Fin de resultados.")
                break

            print(f"    [+] Página {page_count}: Analizando {len(items)} ítems...")

            items_added = 0
            for item in items:
                original_price = regex_analyzer.clean_price(item)
                
                # --- NUEVA LÓGICA DE SANEAMIENTO DE PRECIOS ---
                # Si el precio es simbólico (0-5 euros), intentamos encontrar el real
                price_was_corrected = False
                if original_price < 5.0:
                    real_price = regex_analyzer.try_extract_hidden_price(
                        item.get("title", ""), 
                        item.get("description", "")
                    )
                    
                    if real_price:
                        # ACTUALIZAMOS EL PRECIO EN MEMORIA
                        # Wallapop usa {amount: X, currency: Y} o a veces solo un float
                        item["price"] = {"amount": real_price, "currency": "EUR"}
                        item["_original_price_was"] = original_price # Guardamos backup
                        price_was_corrected = True
                        # print(f"        [CORRECCIÓN] Precio simbólico {original_price}€ -> {real_price}€")
                
                # --- FIN NUEVA LÓGICA ---

                # Solo ignoramos si el precio sigue siendo basura y NO pudimos corregirlo
                # Pero ahora permitimos que calculate_advanced_risk decida si es UNCERTAIN
                if regex_analyzer.clean_price(item) < 1.0 and not price_was_corrected: 
                    # Realmente basura que no pudimos salvar
                    continue 

                item.pop("images", None)

                # 1. Análisis preliminar (Usará el precio CORREGIDO)
                risk_data = calculate_advanced_risk(item)
                market_analysis = risk_data.get("market_analysis", {})
                
                # Si se categorizó como uncertain price, saltamos enriquecimiento
                if market_analysis.get("detected_category") == "UNCERTAIN_PRICE":
                    # Lo guardamos igual para tener historial, pero sin alertas
                    item["enrichment"] = risk_data
                else:
                    # --- SISTEMA DE TRIGGERS ---
                    should_enrich = False
                    enrichment_reason = ""

                    if price_was_corrected:
                        should_enrich = True
                        enrichment_reason = "Price Hidden/Corrected"

                    # Trigger A: Z-Score Anomaly
                    z_score = market_analysis.get("composite_z_score", 0)
                    if z_score < -1.5:
                        should_enrich = True
                        enrichment_reason = f"Z-Score ({z_score:.2f})"

                    # Trigger B: Risky Keywords
                    factors_str = str(risk_data["risk_factors"])
                    if "Risky Payment" in factors_str or "External Contact" in factors_str:
                        should_enrich = True
                        enrichment_reason = "Risky Keywords"

                    # Trigger C: Quality Gap
                    desc_len = len(item.get("description", ""))
                    high_end = ["APPLE", "GAMING", "PREMIUM_ULTRABOOK"]
                    if market_analysis.get("detected_category") in high_end and desc_len < 50:
                        should_enrich = True
                        enrichment_reason = "High Value/Low Quality"

                    # --- EJECUCIÓN DE ENRIQUECIMIENTO ---
                    if should_enrich:
                        user_id = item.get("user", {}).get("id") or item.get("user_id")
                        if user_id:
                            user_details = get_user_details(user_id)
                            
                            reg_date_ts = user_details.get("register_date")
                            if reg_date_ts:
                                days_active = (datetime.now() - datetime.fromtimestamp(reg_date_ts/1000)).days
                                if days_active < 3:
                                    risk_data["risk_score"] += 30
                                    risk_data["risk_factors"].append("New User (<3 days)")
                            
                            if user_details.get("scam_reports", 0) > 0:
                                risk_data["risk_score"] += 50
                                risk_data["risk_factors"].append("User has Scam Reports")
                                
                            risk_data["risk_score"] = min(100, risk_data["risk_score"])

                    item["enrichment"] = risk_data

                # --- CORRECCIÓN GEO Y FECHAS (Igual que antes) ---
                loc = item.get("location", {})
                if "latitude" in loc and "longitude" in loc:
                    loc["geo"] = {"lat": loc["latitude"], "lon": loc["longitude"]}

                ts_created = item.get("created_at") or item.get("creation_date")
                ts_modified = item.get("modified_at") or item.get("modification_date")
                
                created_iso = None
                modified_iso = None
                is_fresh = False
                
                threshold = datetime.now() - timedelta(hours = 24)

                if ts_created:
                    try:
                        dt_c = datetime.fromtimestamp(ts_created / 1000)
                        created_iso = dt_c.isoformat()
                        if dt_c > threshold: is_fresh = True
                    except: pass

                if ts_modified:
                    try:
                        dt_m = datetime.fromtimestamp(ts_modified / 1000)
                        modified_iso = dt_m.isoformat()
                        if dt_m > threshold: is_fresh = True
                    except: pass

                item["timestamps"] = {
                    "crawl_timestamp": datetime.now().isoformat(),
                    "created_at": created_iso,
                    "modified_at": modified_iso
                }

                if not is_fresh: continue
                
                all_items.append(item)
                items_added += 1
                
                # Log de alertas graves
                if risk_data["risk_score"] >= 50:
                    print(f"        [ALERTA] Fraude (Score: {risk_data['risk_score']})")
                    print(f"                 Título: {item.get('title')[:60]}...")
                    mkt = risk_data['market_analysis']
                    print(f"                 Est: {mkt['estimated_market_value']}€ (Z={mkt['composite_z_score']}) | {enrichment_reason}")
            
            print(f"    -> Agregados {items_added}. Total: {len(all_items)}")

            next_page_token = data.get("meta", {}).get("next_page")
            if not next_page_token: break
            page_count += 1
        
    except KeyboardInterrupt:
        print("\n[!] Guardando datos parciales...")
    except Exception as e:
        print(f"[!] Error: {e}")

    if all_items:
        filename = f"wallapop_smart_data_{datetime.now().strftime('%Y%m%d')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            for item in all_items:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")
        print(f"[OK] Guardado en {filename}")

if __name__ == "__main__":
    run_smart_poller()