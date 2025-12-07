import json
import time
import random
import requests
import os
import statistics
import re
from datetime import datetime
from typing import List, Dict, Any, Tuple

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
MAX_ITEMS_TO_FETCH = 10000
STATS_FILE = "market_stats.json"

# Pesos de importancia para la valoración (Juicio de Experto)
WEIGHTS = {
    "cpu": 0.5,      # La CPU define la mitad del valor
    "gpu": 0.3,      # La gráfica es crítica en gaming
    "ram": 0.1,      # La RAM ajusta el precio marginalmente
    "category": 0.1  # La categoría base da el suelo del precio
}

# Límites lógicos de RAM para evitar falsos positivos (ej. 64GB SSD detectado como RAM)
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
            if attempt > 0: time.sleep(attempt * 2)
            response = requests.get(url, params=params, headers=headers, timeout=10)
            return response
        except requests.RequestException:
            continue
    return None

def get_stats_for_component(category_node, component_type, component_name):
    """Busca las estadísticas de un componente específico si existen."""
    if not component_name: return None
    try:
        return category_node["components"][component_type].get(component_name)
    except KeyError:
        return None

def smart_truncate_spam(text: str) -> str:
    """
    Corta el texto si detecta un bloque de etiquetas spam típico de Wallapop.
    Ej: "GTX RTX 3060 4060 ps5 iphone..."
    """
    lines = text.split('\n')
    clean_lines = []
    
    # Lista de palabras que, si aparecen muchas juntas, indican bloque de spam
    spam_indicators = ["rtx", "gtx", "amd", "intel", "ryzen", "i7", "i5", "ps5", "xbox", "iphone", "samsung", "asus", "msi"]
    
    for line in lines:
        # Si una línea tiene más de 3 indicadores de spam distintos, cortamos aquí.
        hits = 0
        line_lower = line.lower()
        for ind in spam_indicators:
            if ind in line_lower:
                hits += 1
        
        if hits > 3:
            # Hemos encontrado el vertedero de keywords. Paramos de leer.
            break
            
        clean_lines.append(line)
        
    return "\n".join(clean_lines)

def sanitize_hardware_ambiguities(text: str) -> str:
    """
    Corrige ambigüedades comunes donde componentes de almacenamiento
    se confunden con procesadores (ej: 'SSD M2' vs 'Apple M2').
    """
    # 1. Proteger "SSD M2" / "Disco M2" -> Reemplazar por "SSD_NVME"
    # Esto elimina el token "M2" aislado que confunde al regex de CPU
    text = re.sub(r"(?i)\b(ssd|disco|disk|drive|almacenamiento)\s+m\.?2\b", r"\1_NVME", text)
    
    # 2. Proteger "M2 NVMe" / "M2 SATA" -> Reemplazar por "NVME_DRIVE"
    text = re.sub(r"(?i)\bm\.?2\s+(ssd|nvme|sata)\b", r"NVME_\1", text)
    
    return text

def apply_category_constraints(specs: Dict, category: str, full_text_original: str) -> Dict:
    """
    Aplica reglas de negocio para corregir detecciones erróneas.
    Ej: Un Chromebook no puede tener 64GB de RAM (probablemente era el disco duro).
    """
    # 1. Corrección de RAM
    limit_ram = RAM_LIMITS.get(category, 128)
    
    current_ram_gb = 0
    if specs.get("ram"):
        try:
            current_ram_gb = int(re.sub(r"[^0-9]", "", specs["ram"]))
        except: pass
        
    # Si la RAM detectada supera el límite lógico de la categoría, re-escaneamos
    # usando el parámetro max_gb de regex_analyzer para buscar un número menor válido.
    if current_ram_gb > limit_ram:
        corrected_ram = regex_analyzer.extract_ram(full_text_original.lower(), max_gb=limit_ram)
        if corrected_ram:
            specs["ram"] = corrected_ram
        else:
            # Si no encontramos otra RAM válida, borramos la incorrecta para no distorsionar
            specs["ram"] = None

    # 2. Corrección de CPU en Chromebooks/Netbooks
    # Si es Chromebook y detectamos "i7" pero también hay "Celeron" en el texto, priorizamos Celeron.
    if category == "CHROMEBOOK" and specs.get("cpu") and "I7" in specs["cpu"]:
        if "celeron" in full_text_original.lower():
            specs["cpu"] = "INTEL CELERON"
        elif "pentium" in full_text_original.lower():
             specs["cpu"] = "INTEL PENTIUM"
             
    return specs

def get_prioritized_specs_and_category(title: str, description: str) -> Tuple[Dict, str]:
    """
    Orquesta la extracción, limpieza de spam y validación de reglas.
    """
    # FASE 0: Sanitización de Ambigüedades (SSD M2 != Apple M2)
    title_clean = sanitize_hardware_ambiguities(title)
    desc_clean_spam = smart_truncate_spam(description)
    desc_clean = sanitize_hardware_ambiguities(desc_clean_spam)
    
    title_lower = title_clean.lower()
    
    # Truncado adicional por seguridad
    desc_truncated = desc_clean[:400] 
    
    # 2. Extracción (Usando texto limpio)
    specs_title = regex_analyzer.extract_specs_regex(title_clean)
    specs_desc = regex_analyzer.extract_specs_regex(desc_truncated)
    
    final_specs = {
        "cpu": specs_title.get("cpu") if specs_title.get("cpu") else specs_desc.get("cpu"),
        "ram": specs_title.get("ram") if specs_title.get("ram") else specs_desc.get("ram"),
        "gpu": specs_title.get("gpu") if specs_title.get("gpu") else specs_desc.get("gpu"),
    }
    
    # 3. Determinación de Categoría (Locking)
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
    
    # 4. APLICACIÓN DE REGLAS DE NEGOCIO
    full_text_for_correction = f"{title_clean} {desc_truncated}"
    final_specs = apply_category_constraints(final_specs, category, full_text_for_correction)
        
    return final_specs, category

# --- LÓGICA DE DETECCIÓN PROFESIONAL (PONDERADA) ---
def calculate_advanced_risk(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calcula el riesgo utilizando un ENSEMBLE PONDERADO de Z-Scores.
    """
    score = 0
    factors = []
    
    title = item.get("title", "")
    desc = item.get("description", "")
    price = regex_analyzer.clean_price(item)
    
    # Pipeline mejorado: Extracción -> Limpieza -> Categorización -> Constraints
    specs, category = get_prioritized_specs_and_category(title, desc)
    
    stats_node = MARKET_STATS.get(category, {})
    
    # 2. Recopilación de señales estadísticas (Ensemble)
    signals = [] 
    
    # A) Señal de CPU
    cpu_stats = get_stats_for_component(stats_node, "cpu", specs.get("cpu"))
    if cpu_stats and cpu_stats["stdev"] > 0:
        z_cpu = (price - cpu_stats["mean"]) / cpu_stats["stdev"]
        signals.append({
            "z": z_cpu, 
            "weight": WEIGHTS["cpu"], 
            "ref_price": cpu_stats["mean"],
            "source": f"CPU:{specs['cpu']}"
        })

    # B) Señal de GPU
    gpu_stats = get_stats_for_component(stats_node, "gpu", specs.get("gpu"))
    if gpu_stats and gpu_stats["stdev"] > 0:
        z_gpu = (price - gpu_stats["mean"]) / gpu_stats["stdev"]
        signals.append({
            "z": z_gpu, 
            "weight": WEIGHTS["gpu"], 
            "ref_price": gpu_stats["mean"],
            "source": f"GPU:{specs['gpu']}"
        })
        
    # C) Señal de RAM
    ram_stats = get_stats_for_component(stats_node, "ram", specs.get("ram"))
    if ram_stats and ram_stats["stdev"] > 0:
        z_ram = (price - ram_stats["mean"]) / ram_stats["stdev"]
        signals.append({
            "z": z_ram, 
            "weight": WEIGHTS["ram"], 
            "ref_price": ram_stats["mean"],
            "source": f"RAM:{specs['ram']}"
        })

    # D) Señal de Categoría Base 
    if stats_node.get("stdev", 0) > 0:
        z_cat = (price - stats_node["mean"]) / stats_node["stdev"]
        signals.append({
            "z": z_cat, 
            "weight": WEIGHTS["category"], 
            "ref_price": stats_node["mean"],
            "source": f"CAT:{category}"
        })

    # 3. Cálculo del Z-Score Ponderado
    final_z_score = 0
    total_weight = 0
    estimated_market_value = 0
    
    if signals:
        weighted_z_sum = 0
        weighted_price_sum = 0
        
        for s in signals:
            weighted_z_sum += s["z"] * s["weight"]
            weighted_price_sum += s["ref_price"] * s["weight"]
            total_weight += s["weight"]
            
        if total_weight > 0:
            final_z_score = weighted_z_sum / total_weight
            estimated_market_value = weighted_price_sum / total_weight
    
    # 4. Interpretación del Riesgo Compuesto
    
    if final_z_score < -1.5:
        score += 30
        factors.append(f"Statistically Cheap (Combined Z={final_z_score:.2f})")
        
    if final_z_score < -2.5: # Anomalía muy fuerte
        score += 40 
        factors.append(f"EXTREME Price Anomaly (Target ~{int(estimated_market_value)}€)")

    # Fallback: Ratio simple
    if estimated_market_value > 0 and price > 0:
        ratio = price / estimated_market_value
        if ratio < 0.4:
            score += 20
            factors.append(f"Price is <40% of est. value ({int(ratio*100)}%)")

    # 5. Heurísticas Clásicas (Keywords)
    suspicious_keywords = ["urgente", "roto", "bloqueado", "bios", "icloud", "pieza", "tarada"]
    found_kws = [kw for kw in suspicious_keywords if kw in (title + " " + desc).lower()]
    
    if found_kws:
        score += 20
        factors.append(f"Suspicious keywords: {found_kws}")
    
    # 6. Usuario Nuevo
    user = item.get("user", {})
    if user.get("register_date"):
        try:
            reg_date = datetime.fromtimestamp(user["register_date"] / 1000)
            if (datetime.now() - reg_date).days < 2:
                score += 30
                factors.append("User registered < 48h ago")
        except: pass

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
    
    print("--- INICIANDO SMART POLLER (Modelo Estadístico Ponderado + Anti-Spam) ---")
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
                if regex_analyzer.clean_price(item) < 20: continue 
                
                item.pop("images", None)

                risk_data = calculate_advanced_risk(item)
                
                item["enrichment"] = risk_data
                item["timestamps"] = {"crawl_timestamp": datetime.now().isoformat()}
                
                all_items.append(item)
                items_added += 1
                
                if risk_data["risk_score"] >= 50:
                    print(f"        [ALERTA] Fraude (Score: {risk_data['risk_score']})")
                    print(f"                 Título: {item.get('title')[:60]}...")
                    mkt = risk_data['market_analysis']
                    price_val = item.get('price', {}).get('amount')
                    print(f"                 Precio: {price_val}€ vs Est: {mkt['estimated_market_value']}€ (Z={mkt['composite_z_score']})")
                    print(f"                 Motivos: {risk_data['risk_factors']}")
            
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