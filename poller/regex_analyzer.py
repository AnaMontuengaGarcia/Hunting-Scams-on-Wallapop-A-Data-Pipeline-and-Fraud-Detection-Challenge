import json
import statistics
import re
import glob
import os
from collections import defaultdict
from typing import List, Dict, Set, Any, Tuple, Optional

# --- CONFIGURACIÓN ---
OUTPUT_STATS_FILE = "market_stats.json"

RAM_LIMITS = {
    "CHROMEBOOK": 16,
    "SURFACE": 32,
    "PREMIUM_ULTRABOOK": 64,
    "GENERICO": 64
}

# --- REGEX DE PRECIOS ---
RE_HIDDEN_PRICE = re.compile(r'(?i)(?:precio|valor|vende|vendo|pido|oferta)[:\s]*(?:por)?\s*(\d{2,4})(?:[\.,]\d{2})?\s*(?:€|eur|euros)', re.IGNORECASE)
RE_LOOSE_PRICE = re.compile(r'\b(\d{2,4})\s*(?:€|euros)\b', re.IGNORECASE)

# --- REGEX DE ESTADO (Fallback) ---
RE_CONDITION_NEW = re.compile(r'\b(nuevo|precintado|sin abrir|estrenar|sealed|new|garantia|factura)\b', re.IGNORECASE)
RE_CONDITION_LIKE_NEW = re.compile(r'\b(como nuevo|impecable|perfecto estado|reacondicionado|refurbished|poquisimo uso|sin uso)\b', re.IGNORECASE)
RE_CONDITION_BROKEN = re.compile(r'\b(roto|averiado|fallo|bloqueado|icloud|bios|pantalla rota|no enciende|no funciona|para piezas|despiece|repuesto|tarada|golpe|mojado|water|broken|parts|read|leer|reparar)\b', re.IGNORECASE)

# --- REGEX DE HARDWARE ---
RE_RAM = re.compile(r'\b(\d+)\s*(?:gb|gigas?)\b(?!\s*(?:[\.,\-\/]\s*)?(?:de\s+)?(?:ssd|hdd|emmc|rom|almacenamiento|storage|disco|nvme|flash|interno|interna))', re.IGNORECASE)
RE_CPU_BRAND = re.compile(r'\b(intel|amd|apple|qualcomm|microsoft)\b', re.IGNORECASE)
RE_CPU_MODELS = [
    re.compile(r'\b(core\s*-?)?(i[3579])\b', re.IGNORECASE),      
    re.compile(r'\b(ryzen)\s*-?([3579])\b', re.IGNORECASE),       
    re.compile(r'\b(m[123])\s*(pro|max|ultra)?\b', re.IGNORECASE),
    re.compile(r'\b(celeron|pentium|atom|xeon)\b', re.IGNORECASE),
    re.compile(r'\b(snapdragon|sq[123])\b', re.IGNORECASE)
]
RE_GPU_BRAND = re.compile(r'\b(nvidia|amd|radeon|geforce)\b', re.IGNORECASE)
RE_GPU_MODEL = re.compile(r'\b((?:rtx|gtx|rx)\s*-?\d{3,4}[a-z]*)\b', re.IGNORECASE)

# --- CLASIFICACIÓN DE CATEGORÍAS ---
SUB_CATEGORIES_RULES = {
    "APPLE": ["macbook", "mac", "apple", "macos"],
    "SURFACE": ["surface", "microsoft surface"], 
    "WORKSTATION": ["thinkpad", "latitude", "precision", "zbook", "quadro", "elitebook", "probook"],
    "PREMIUM_ULTRABOOK": ["xps", "spectre", "zenbook", "gram", "yoga", "matebook"], 
    "GAMING": ["gaming", "gamer", "rog", "tuf", "alienware", "msi", "omen", "predator", "legion", "nitro", "victus", "loq", "blade", "razer"],
    "CHROMEBOOK": ["chromebook", "chrome"]
}

def clean_price(item):
    try:
        p = item.get("price", 0)
        if isinstance(p, dict): p = float(p.get("amount", 0))
        return float(p)
    except: return 0.0

def try_extract_hidden_price(title: str, description: str) -> Optional[float]:
    full_text = f"{title} \n {description}"
    matches = RE_HIDDEN_PRICE.findall(full_text)
    for m in matches:
        try:
            val = float(m)
            if val > 20: return val
        except: pass
    matches_loose = RE_LOOSE_PRICE.findall(full_text)
    candidates = []
    for m in matches_loose:
        try:
            val = float(m)
            if 50 <= val <= 5000: candidates.append(val)
        except: pass
    if candidates: return max(candidates)
    return None

def is_match(text_lower, keywords):
    for kw in keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', text_lower): return True
    return False

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

def detect_condition_from_data(item: Dict, full_text_lower: str) -> str:
    """
    Intenta sacar la condición de la API (campos guardados por analist_poller) 
    y hace fallback a Regex si no existen.
    """
    # 1. API Field 'type_attributes' (Lo más fiable)
    api_cond = None
    try:
        api_cond = item.get("type_attributes", {}).get("condition", {}).get("value")
    except AttributeError:
        pass # type_attributes podría ser None

    if api_cond:
        if api_cond == "new": return "NEW"
        if api_cond == "as_good_as_new": return "LIKE_NEW"
        if api_cond == "has_given_it_all": return "BROKEN"
        # "good", "fair" -> USED
        return "USED"

    # 2. API Field 'is_refurbished' Flag
    # Corrección: Manejo robusto de None
    is_refurbished_data = item.get("is_refurbished")
    if is_refurbished_data and isinstance(is_refurbished_data, dict) and is_refurbished_data.get("flag") is True:
        return "LIKE_NEW"

    # 3. Fallback Regex (Si el dato de API no existe o es antiguo)
    if RE_CONDITION_BROKEN.search(full_text_lower): return "BROKEN"
    if RE_CONDITION_NEW.search(full_text_lower): return "NEW"
    if RE_CONDITION_LIKE_NEW.search(full_text_lower): return "LIKE_NEW"
    
    return "USED"

def apply_category_constraints(specs: Dict, category: str, full_text_original: str) -> Dict:
    limit_ram = RAM_LIMITS.get(category, 128)
    current_ram_gb = 0
    if specs.get("ram"):
        try:
            current_ram_gb = int(re.sub(r"[^0-9]", "", specs["ram"]))
        except: pass
        
    if current_ram_gb > limit_ram:
        corrected_ram = extract_ram(full_text_original.lower(), max_gb=limit_ram)
        specs["ram"] = corrected_ram if corrected_ram else None

    if category == "CHROMEBOOK" and specs.get("cpu") and "I7" in specs["cpu"]:
        if "celeron" in full_text_original.lower(): specs["cpu"] = "INTEL CELERON"
        elif "pentium" in full_text_original.lower(): specs["cpu"] = "INTEL PENTIUM"
             
    return specs

def is_valid_ram(ram_val): return ram_val in [4, 6, 8, 12, 16, 20, 24, 32, 40, 48, 64]

def clean_cpu_string(brand, models, is_apple):
    models = sorted(list(models), reverse=True)
    if not models: return None
    best = models[0].upper() 
    if is_apple or "M1" in best or "M2" in best or "M3" in best: brand = "APPLE"
    elif "RYZEN" in best: brand = "AMD"
    elif best.startswith("I") and len(best) >= 2 and best[1].isdigit(): brand = "INTEL"
    elif any(x in best for x in ["CELERON", "PENTIUM", "ATOM", "XEON"]): brand = "INTEL"
    elif any(x in best for x in ["SNAPDRAGON", "SQ1", "SQ2", "SQ3"]): brand = "QUALCOMM"
    if "RYZEN" in best and best.replace("RYZEN", "") and best.replace("RYZEN", "")[0].isdigit(): best = best.replace("RYZEN", "RYZEN ")
    if brand == "APPLE" and not best.startswith("APPLE"): return f"APPLE {best}"
    return f"{brand} {best}".strip() if brand else best

def clean_gpu_string(brand, models):
    models = sorted(list(models), reverse=True)
    if not models: return None
    best = models[0].upper()
    match = re.match(r"^([A-Z]+)(\d.*)$", best)
    if match and " " not in best: best = f"{match.group(1)} {match.group(2)}"
    if any(x in best for x in ["RTX", "GTX", "MX", "QUADRO"]): brand = "NVIDIA"
    elif any(x in best for x in ["RX", "RADEON", "FIREPRO"]): brand = "AMD"
    final = best.replace(brand or "", "").strip()
    return f"{brand} {final}".strip() if brand else final

def extract_ram(text_lower, max_gb=128):
    ram_matches = RE_RAM.findall(text_lower)
    best_ram = 0
    ram_str = None
    for val_str in ram_matches:
        try:
            val = int(val_str)
            if is_valid_ram(val) and val <= max_gb:
                if val > best_ram:
                    best_ram = val
                    ram_str = f"{val}GB"
        except: pass
    return ram_str

def extract_specs_regex(text):
    text_lower = text.lower()
    specs = {"ram": None, "cpu_brand": None, "cpu_models": set(), "gpu_brand": None, "gpu_models": set(), "is_apple": False}
    specs["ram"] = extract_ram(text_lower)
    brand_matches = RE_CPU_BRAND.findall(text_lower)
    if brand_matches: specs["cpu_brand"] = brand_matches[0].upper() 

    for pattern in RE_CPU_MODELS:
        matches = pattern.findall(text_lower)
        for m in matches:
            full_model = ""
            if isinstance(m, tuple):
                parts = [p for p in m if p]
                if parts[0].lower().startswith("m") and len(parts) > 1: full_model = f"{parts[0]} {parts[1]}"
                else: full_model = "".join(parts).replace(" ", "").replace("-", "")
            else: full_model = m.replace(" ", "")
            if "ryzen" in full_model.lower(): specs["cpu_models"].add(f"RYZEN{re.sub(r'[^0-9]', '', full_model)}")
            elif full_model.lower().startswith("m") and full_model[1].isdigit():
                specs["cpu_models"].add(full_model.upper())
                specs["is_apple"] = True
            elif full_model.lower().startswith("i") and full_model[1].isdigit(): specs["cpu_models"].add(full_model.upper())
            elif any(x in full_model.lower() for x in ["celeron", "pentium", "atom", "xeon", "snapdragon", "sq1", "sq2", "sq3"]):
                 specs["cpu_models"].add(full_model.upper())

    gpu_brand_matches = RE_GPU_BRAND.findall(text_lower)
    if gpu_brand_matches:
        specs["gpu_brand"] = gpu_brand_matches[0].upper()
        if specs["gpu_brand"] in ["GEFORCE"]: specs["gpu_brand"] = "NVIDIA"
    gpu_model_matches = RE_GPU_MODEL.findall(text_lower)
    for gm in gpu_model_matches: specs["gpu_models"].add(gm.upper())

    has_pc_cpu = (specs["cpu_brand"] in ["INTEL", "AMD"]) or any((m.startswith("I") and m[1:].isdigit()) or "RYZEN" in m for m in specs["cpu_models"])
    if has_pc_cpu and specs["is_apple"]:
        specs["cpu_models"] = {m for m in specs["cpu_models"] if not re.match(r"^M[123].*$", m)}
        specs["is_apple"] = False
    if specs["is_apple"]:
        specs["cpu_brand"] = "APPLE"
        specs["cpu_models"] = {m for m in specs["cpu_models"] if re.match(r"^M[123]", m)}

    return {
        "cpu": clean_cpu_string(specs["cpu_brand"], specs["cpu_models"], specs["is_apple"]),
        "ram": specs["ram"],
        "gpu": clean_gpu_string(specs["gpu_brand"], specs["gpu_models"])
    }

def classify_prime_category(text_lower, specs):
    cpu_str = (specs.get("cpu") or "").upper()
    if "APPLE M" in cpu_str: return "APPLE"
    if specs.get("gpu") and "quadro" in specs["gpu"].lower(): return "WORKSTATION"
    if specs.get("gpu"): return "GAMING"
    if "apple" in specs.get("cpu_brand", "").lower() or "macbook" in text_lower or "macos" in text_lower:
        if "AMD" not in cpu_str: return "APPLE"
    for cat, kws in SUB_CATEGORIES_RULES.items():
        if cat in ["GAMING", "APPLE"]: continue
        if is_match(text_lower, kws): return cat
    if "gaming" in text_lower: return "GAMING"
    return "GENERICO"

def get_prioritized_specs_and_category(title: str, description: str) -> Tuple[Dict, str, str]:
    """
    Función helper para análisis en tiempo real (Poller).
    Devuelve specs, categoría y condición detectada por Regex (ya que Poller no siempre tiene API details a mano en la primera pasada).
    """
    title_clean = sanitize_hardware_ambiguities(title)
    desc_clean = sanitize_hardware_ambiguities(smart_truncate_spam(description))
    
    full_text = f"{title_clean} {desc_clean}".lower()
    
    # 1. Extracción Specs
    specs_title = extract_specs_regex(title_clean)
    specs_desc = extract_specs_regex(desc_clean[:400])
    final_specs = {
        "cpu": specs_title.get("cpu") if specs_title.get("cpu") else specs_desc.get("cpu"),
        "ram": specs_title.get("ram") if specs_title.get("ram") else specs_desc.get("ram"),
        "gpu": specs_title.get("gpu") if specs_title.get("gpu") else specs_desc.get("gpu"),
    }
    
    # 2. Clasificación Categoría
    cat = "GENERICO"
    if "chromebook" in title_clean.lower(): cat = "CHROMEBOOK"
    elif any(x in title_clean.lower() for x in ["macbook", "mac air", "mac pro", "imac"]): cat = "APPLE"
    elif "surface" in title_clean.lower(): cat = "SURFACE"
    else: cat = classify_prime_category(full_text, final_specs)
    
    final_specs = apply_category_constraints(final_specs, cat, full_text)
    
    # 3. Detección Condición (Solo Regex aquí)
    cond = "USED"
    if RE_CONDITION_BROKEN.search(full_text): cond = "BROKEN"
    elif RE_CONDITION_NEW.search(full_text): cond = "NEW"
    elif RE_CONDITION_LIKE_NEW.search(full_text): cond = "LIKE_NEW"
    
    return final_specs, cat, cond

def determine_market_segment(title_lower, price, condition, specs):
    if price < 5: return "UNCERTAIN"
    if price > 10000: return "JUNK"
    
    if condition == "BROKEN": return "BROKEN" # Forzar segmento
    
    is_laptop = False 
    for ind in ["portatil", "laptop", "macbook"]: 
        if ind in title_lower: is_laptop = True

    is_accessory = False
    for kw in ["funda", "caja", "dock", "raton"]:
        if kw in title_lower: is_accessory = True
    
    if is_accessory and price < 100: return "ACCESSORY"
    if is_accessory and not is_laptop: return "ACCESSORY"
    
    return "PRIME"

def process_data(input_file):
    print(f"[*] Generando Estadísticas (Anidadas por Estado: NEW/USED/etc) desde {input_file}...")
    with open(input_file, "r", encoding="utf-8") as f:
        items = json.load(f)

    # Estructura: CATEGORY -> CONDITION -> SPECS -> [Prices]
    market_data = {
        "PRIME": defaultdict(lambda: defaultdict(lambda: {"prices": [], "specs": {"cpu": defaultdict(list), "ram": defaultdict(list), "gpu": defaultdict(list)}})),
        "SECONDARY": defaultdict(list), 
        "UNCERTAIN": {"prices": []}     
    }

    count = 0
    for item in items:
        price = clean_price(item)
        title = item.get('title', '') or ""
        desc = item.get('description', '') or ""
        
        # 1. Extracción de specs y cat base
        specs, cat, _ = get_prioritized_specs_and_category(title, desc)
        
        # 2. Detección Inteligente de Condición (API > Regex)
        full_text = (title + " " + desc).lower()
        # AQUÍ ESTÁ LA CLAVE: Usamos la función que mira los campos que analist_poller ha guardado
        condition = detect_condition_from_data(item, full_text)
        
        # 3. Segmentación
        segment = determine_market_segment(title.lower(), price, condition, specs)
        
        if segment == "JUNK": continue
        if segment == "UNCERTAIN" or (not specs["cpu"] and not specs["ram"]):
            market_data["UNCERTAIN"]["prices"].append(price)
            continue
        if segment in ["BROKEN", "ACCESSORY"]:
            if segment == "BROKEN": market_data["SECONDARY"]["BROKEN"].append(price)
            else: market_data["SECONDARY"]["ACCESSORY"].append(price)
            continue
            
        # Agrupación Nested
        group = market_data["PRIME"][cat][condition]
        group["prices"].append(price)
        if specs["cpu"]: group["specs"]["cpu"][specs["cpu"]].append(price)
        if specs["ram"]: group["specs"]["ram"][specs["ram"]].append(price)
        if specs["gpu"]: group["specs"]["gpu"][specs["gpu"]].append(price)
        count += 1

    print(f"[*] Procesados {count} items válidos para PRIME stats.")

    # Generación de JSON final
    final_stats = {}
    for cat, cond_dict in market_data["PRIME"].items():
        final_stats[cat] = {}
        for cond, data in cond_dict.items():
            prices = data["prices"]
            if len(prices) < 2: continue 
            
            stats = {
                "mean": round(statistics.mean(prices), 2),
                "median": round(statistics.median(prices), 2),
                "stdev": round(statistics.stdev(prices), 2) if len(prices)>1 else 0,
                "count": len(prices),
                "components": {}
            }
            # Componentes
            for ctype, cdata in data["specs"].items():
                stats["components"][ctype] = {}
                for cname, cprices in cdata.items():
                    if len(cprices) >= 2:
                        stats["components"][ctype][cname] = {
                            "mean": round(statistics.mean(cprices), 2),
                            "median": round(statistics.median(cprices), 2),
                            "stdev": round(statistics.stdev(cprices), 2) if len(cprices)>1 else 0,
                            "count": len(cprices)
                        }
            final_stats[cat][cond] = stats
            
    # Añadir Secondary/Uncertain al final
    for sec_cat, prices in market_data["SECONDARY"].items():
         if len(prices) > 3:
            final_stats[sec_cat] = {
                "mean": round(statistics.mean(prices), 2),
                "count": len(prices)
            }
            
    unc_prices = market_data["UNCERTAIN"]["prices"]
    if len(unc_prices) > 3:
        final_stats["UNCERTAIN"] = {
            "mean": round(statistics.mean(unc_prices), 2),
            "count": len(unc_prices)
        }

    with open(OUTPUT_STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_stats, f, indent=4)
        
    print(f"[OK] Guardado en {OUTPUT_STATS_FILE}")

if __name__ == "__main__":
    list_of_files = glob.glob('wallapop_raw_full_*.json') # Prioridad a los full fetch
    if not list_of_files: list_of_files = glob.glob('wallapop_raw_data_*.json')
    
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        process_data(latest_file)
    else:
        print("[!] No hay datos raw para procesar.")