import json
import statistics
import re
import glob
import os
from collections import defaultdict
from typing import List, Dict, Set, Any, Tuple, Optional

# --- CONFIGURACIÓN ---
OUTPUT_STATS_FILE = "market_stats.json"

# Límites lógicos de RAM para evitar falsos positivos
RAM_LIMITS = {
    "CHROMEBOOK": 16,
    "SURFACE": 32,
    "PREMIUM_ULTRABOOK": 64,
    "GENERICO": 64
}

# --- EXPRESIONES REGULARES DE PRECIOS OCULTOS ---
# Busca: "precio 500€", "500 euros", "vendo por 500", "valor 500"
# Prioriza números de 2 o 3 cifras para evitar confundir con GBs o modelos
RE_HIDDEN_PRICE = re.compile(
    r'(?i)(?:precio|valor|vende|vendo|pido|oferta)[:\s]*(?:por)?\s*(\d{2,4})(?:[\.,]\d{2})?\s*(?:€|eur|euros)', 
    re.IGNORECASE
)
# Busca precios sueltos con símbolo de moneda fuertes: "500€" o "500 €" (evita "500")
RE_LOOSE_PRICE = re.compile(
    r'\b(\d{2,4})\s*(?:€|euros)\b', 
    re.IGNORECASE
)

# --- EXPRESIONES REGULARES COMPILADAS (HARDWARE) ---

# 1. RAM: Captura "8GB", "8 gb", "16 gigas"
RE_RAM = re.compile(
    r'\b(\d+)\s*(?:gb|gigas?)\b(?!\s*(?:[\.,\-\/]\s*)?(?:de\s+)?(?:ssd|hdd|emmc|rom|almacenamiento|storage|disco|nvme|flash|interno|interna))', 
    re.IGNORECASE
)

# 2. CPU: Patrones para detectar marcas y modelos
RE_CPU_BRAND = re.compile(r'\b(intel|amd|apple|qualcomm|microsoft)\b', re.IGNORECASE)
RE_CPU_MODELS = [
    re.compile(r'\b(core\s*-?)?(i[3579])\b', re.IGNORECASE),      
    re.compile(r'\b(ryzen)\s*-?([3579])\b', re.IGNORECASE),       
    re.compile(r'\b(m[123])\s*(pro|max|ultra)?\b', re.IGNORECASE),
    re.compile(r'\b(celeron|pentium|atom|xeon)\b', re.IGNORECASE),
    re.compile(r'\b(snapdragon|sq[123])\b', re.IGNORECASE)
]

# 3. GPU: Patrones para gráficas
RE_GPU_BRAND = re.compile(r'\b(nvidia|amd|radeon|geforce)\b', re.IGNORECASE)
RE_GPU_MODEL = re.compile(r'\b((?:rtx|gtx|rx)\s*-?\d{3,4}[a-z]*)\b', re.IGNORECASE)

# --- PALABRAS CLAVE ---
BROKEN_KEYWORDS = [
    "roto", "averiado", "fallo", "bloqueado", "icloud", "bios", "pantalla rota", 
    "no enciende", "no funciona", "para piezas", "despiece", "repuesto", "tarada", 
    "golpe", "mojado", "water", "broken", "parts", "read", "leer", "reparar"
]

ACCESSORY_KEYWORDS = [
    "funda", "carcasa", "maletin", "mochila", "bag", "sleeve", "skin",
    "dock", "docking", "hub", "base", "soporte", "stand", "cooler", "ventilador",
    "caja", "embalaje", "box", "vacia",
    "raton", "mouse", "alfombrilla", 
    "stylus", "lapiz", "pen",
    "protector", "cristal", "pegatina"
]

LAPTOP_INDICATORS = [
    "portatil", "laptop", "macbook", "ordenador", "pc", "computer", 
    "notebook", "netbook", "ultrabook", "convertible", "2 en 1", "2 in 1",
    "surface pro", "thinkpad", "latitude", "precision", "xps", "inspiron",
    "zenbook", "vivobook", "rog", "tuf", "zephyrus", "legion", "ideapad", "yoga",
    "pavilion", "omen", "victus", "envy", "spectre", "elitebook", "probook",
    "matebook", "magicbook", "galaxy book", "prestige", "modern", "katana", "cyborg"
]

# Definición de categorías. 
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
    """
    Intenta encontrar un precio real en el texto cuando el precio listado es sospechoso (0-5€).
    Devuelve el precio encontrado o None.
    """
    full_text = f"{title} \n {description}"
    
    # 1. Intentar búsqueda con contexto explícito ("precio 500€")
    matches = RE_HIDDEN_PRICE.findall(full_text)
    for m in matches:
        try:
            val = float(m)
            if val > 20: # Filtrar precios de envíos o tonterías
                return val
        except: pass
        
    # 2. Intentar búsqueda de precios sueltos ("500€")
    matches_loose = RE_LOOSE_PRICE.findall(full_text)
    # Preferimos el valor más alto encontrado que parezca un precio de portátil
    candidates = []
    for m in matches_loose:
        try:
            val = float(m)
            if 50 <= val <= 5000: # Rango razonable
                candidates.append(val)
        except: pass
    
    if candidates:
        return max(candidates) # Devolvemos el más alto (asumiendo que otros pueden ser cuotas o descuentos)
        
    return None

def is_match(text_lower, keywords):
    for kw in keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
            return True
    return False

# --- NUEVAS FUNCIONES DE SANITIZACIÓN ---

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
        corrected_ram = extract_ram(full_text_original.lower(), max_gb=limit_ram)
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

def determine_market_segment(title, description, price, specs=None):
    title_lower = title.lower()
    
    if price < 5: return "UNCERTAIN" # Cambio: De JUNK a UNCERTAIN si no se corrigió
    if price > 10000: return "JUNK"
    
    if is_match(title_lower, BROKEN_KEYWORDS):
        return "BROKEN"
        
    is_laptop = is_match(title_lower, LAPTOP_INDICATORS)
    is_accessory_keyword = is_match(title_lower, ACCESSORY_KEYWORDS)
    
    if is_accessory_keyword:
        if price < 100: return "ACCESSORY"
        has_specs = specs and (specs.get("cpu") or specs.get("ram"))
        if price > 200 and has_specs: return "PRIME"
        if any(title_lower.startswith(x) for x in ["funda", "carcasa", "caja", "dock", "base"]): return "ACCESSORY"
        if is_laptop: return "PRIME"
        return "ACCESSORY"

    component_keywords = ["pantalla", "teclado", "bateria", "cargador", "placa base", "motherboard", "disco", "ssd", "ram"]
    if is_match(title_lower, component_keywords) and not is_laptop:
        return "ACCESSORY"

    return "PRIME"

def is_valid_ram(ram_val):
    return ram_val in [4, 6, 8, 12, 16, 20, 24, 32, 40, 48, 64]

def clean_cpu_string(brand, models, is_apple):
    models = sorted(list(models), reverse=True)
    if not models: return None
    best = models[0].upper() 
    
    if is_apple or "M1" in best or "M2" in best or "M3" in best: 
        brand = "APPLE"
    elif "RYZEN" in best: 
        brand = "AMD"
    elif best.startswith("I") and len(best) >= 2 and best[1].isdigit(): 
        brand = "INTEL"
    elif any(x in best for x in ["CELERON", "PENTIUM", "ATOM", "XEON"]):
        brand = "INTEL"
    elif any(x in best for x in ["SNAPDRAGON", "SQ1", "SQ2", "SQ3"]):
        brand = "QUALCOMM"
    
    if "RYZEN" in best and best.replace("RYZEN", "") and best.replace("RYZEN", "")[0].isdigit():
         best = best.replace("RYZEN", "RYZEN ")
    
    if brand == "APPLE" and not best.startswith("APPLE"):
        return f"APPLE {best}"

    return f"{brand} {best}".strip() if brand else best

def clean_gpu_string(brand, models):
    models = sorted(list(models), reverse=True)
    if not models: return None
    best = models[0].upper()
    
    match = re.match(r"^([A-Z]+)(\d.*)$", best)
    if match and " " not in best: 
        best = f"{match.group(1)} {match.group(2)}"
    
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
    specs = {
        "ram": None, 
        "cpu_brand": None, 
        "cpu_models": set(), 
        "gpu_brand": None, 
        "gpu_models": set(), 
        "is_apple": False
    }

    specs["ram"] = extract_ram(text_lower)

    brand_matches = RE_CPU_BRAND.findall(text_lower)
    if brand_matches:
        specs["cpu_brand"] = brand_matches[0].upper() 

    for pattern in RE_CPU_MODELS:
        matches = pattern.findall(text_lower)
        for m in matches:
            full_model = ""
            if isinstance(m, tuple):
                parts = [p for p in m if p]
                if parts[0].lower().startswith("m") and len(parts) > 1:
                     full_model = f"{parts[0]} {parts[1]}"
                else:
                     full_model = "".join(parts).replace(" ", "").replace("-", "")
            else:
                full_model = m.replace(" ", "")
            
            if "ryzen" in full_model.lower():
                num = re.sub(r"[^0-9]", "", full_model)
                specs["cpu_models"].add(f"RYZEN{num}")
            elif full_model.lower().startswith("m") and full_model[1].isdigit():
                specs["cpu_models"].add(full_model.upper())
                specs["is_apple"] = True
            elif full_model.lower().startswith("i") and full_model[1].isdigit():
                specs["cpu_models"].add(full_model.upper())
            elif any(x in full_model.lower() for x in ["celeron", "pentium", "atom", "xeon", "snapdragon", "sq1", "sq2", "sq3"]):
                 specs["cpu_models"].add(full_model.upper())

    gpu_brand_matches = RE_GPU_BRAND.findall(text_lower)
    if gpu_brand_matches:
        specs["gpu_brand"] = gpu_brand_matches[0].upper()
        if specs["gpu_brand"] in ["GEFORCE"]: specs["gpu_brand"] = "NVIDIA"

    gpu_model_matches = RE_GPU_MODEL.findall(text_lower)
    for gm in gpu_model_matches:
        specs["gpu_models"].add(gm.upper())

    has_pc_cpu = (specs["cpu_brand"] in ["INTEL", "AMD"]) or any((m.startswith("I") and m[1:].isdigit()) or "RYZEN" in m for m in specs["cpu_models"])
    
    if has_pc_cpu and specs["is_apple"]:
        specs["cpu_models"] = {m for m in specs["cpu_models"] if not re.match(r"^M[123].*$", m)}
        specs["is_apple"] = False
        
    if specs["is_apple"]:
        specs["cpu_brand"] = "APPLE"
        specs["cpu_models"] = {m for m in specs["cpu_models"] if re.match(r"^M[123]", m)}

    final = {
        "cpu": clean_cpu_string(specs["cpu_brand"], specs["cpu_models"], specs["is_apple"]),
        "ram": specs["ram"],
        "gpu": clean_gpu_string(specs["gpu_brand"], specs["gpu_models"])
    }
    return final

def classify_prime_category(text_lower, specs):
    cpu_str = (specs.get("cpu") or "").upper()
    
    if "APPLE M" in cpu_str: return "APPLE"

    if specs.get("gpu"):
        gpu_str = specs["gpu"].lower()
        if "quadro" in gpu_str: return "WORKSTATION"
        return "GAMING"
    
    if "apple" in specs.get("cpu_brand", "").lower() or "macbook" in text_lower or "macos" in text_lower:
        if "AMD" in cpu_str: pass 
        else: return "APPLE"
        
    for cat, kws in SUB_CATEGORIES_RULES.items():
        if cat in ["GAMING", "APPLE"]: continue
        if is_match(text_lower, kws): return cat
        
    if "gaming" in text_lower: return "GAMING"
    return "GENERICO"

def get_prioritized_specs_and_category(title: str, description: str) -> Tuple[Dict, str]:
    title_clean = sanitize_hardware_ambiguities(title)
    desc_clean_spam = smart_truncate_spam(description)
    desc_clean = sanitize_hardware_ambiguities(desc_clean_spam)
    title_lower = title_clean.lower()
    desc_truncated = desc_clean[:400] 
    
    specs_title = extract_specs_regex(title_clean)
    specs_desc = extract_specs_regex(desc_truncated)
    
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
        category = classify_prime_category(full_text_clean.lower(), final_specs)
    
    full_text_for_correction = f"{title_clean} {desc_truncated}"
    final_specs = apply_category_constraints(final_specs, category, full_text_for_correction)
        
    return final_specs, category

def process_data(input_file):
    print(f"[*] Modo Regex Optimizado (Sincronizado). Leyendo {input_file}...")
    
    with open(input_file, "r", encoding="utf-8") as f:
        items = json.load(f)

    market_data = {
        "PRIME": defaultdict(lambda: {"prices": [], "specs": {"cpu": defaultdict(list), "ram": defaultdict(list), "gpu": defaultdict(list)}}),
        "SECONDARY": defaultdict(list), 
        "UNCERTAIN": {"prices": []}     
    }

    print(f"[*] Procesando {len(items)} ítems...")
    
    for item in items:
        price = clean_price(item)
        title = item.get('title', '') or ""
        desc = item.get('description', '') or ""
        
        specs, cat = get_prioritized_specs_and_category(title, desc)
        
        segment = determine_market_segment(title, desc, price, specs)
        
        if segment == "JUNK": continue
        
        if segment in ["BROKEN", "ACCESSORY"]:
            market_data["SECONDARY"][segment].append(price)
            continue
            
        if segment == "UNCERTAIN" or (not specs["cpu"] and not specs["ram"]):
            market_data["UNCERTAIN"]["prices"].append(price)
            continue
            
        group = market_data["PRIME"][cat]
        group["prices"].append(price)
        if specs["cpu"]: group["specs"]["cpu"][specs["cpu"]].append(price)
        if specs["ram"]: group["specs"]["ram"][specs["ram"]].append(price)
        if specs["gpu"]: group["specs"]["gpu"][specs["gpu"]].append(price)

    final_stats = {}
    
    for cat, data in market_data["PRIME"].items():
        prices = data["prices"]
        if len(prices) < 3: continue
        
        stats = {
            "mean": round(statistics.mean(prices), 2),
            "median": round(statistics.median(prices), 2),
            "stdev": round(statistics.stdev(prices), 2) if len(prices)>1 else 0,
            "count": len(prices),
            "components": {}
        }
        
        for comp_type, comp_data in data["specs"].items():
            stats["components"][comp_type] = {}
            for comp_name, comp_prices in comp_data.items():
                if len(comp_prices) >= 3:
                    stats["components"][comp_type][comp_name] = {
                        "mean": round(statistics.mean(comp_prices), 2),
                        "median": round(statistics.median(comp_prices), 2),
                        "stdev": round(statistics.stdev(comp_prices), 2) if len(comp_prices)>1 else 0,
                        "count": len(comp_prices)
                    }
        final_stats[cat] = stats

    for cat, prices in market_data["SECONDARY"].items():
        if len(prices) < 3: continue
        final_stats[cat] = {
            "mean": round(statistics.mean(prices), 2),
            "median": round(statistics.median(prices), 2),
            "stdev": round(statistics.stdev(prices), 2) if len(prices)>1 else 0,
            "count": len(prices),
            "note": "Secondary Market (Accessories/Broken)"
        }

    unc_prices = market_data["UNCERTAIN"]["prices"]
    if len(unc_prices) > 3:
        final_stats["UNCERTAIN"] = {
            "mean": round(statistics.mean(unc_prices), 2),
            "median": round(statistics.median(unc_prices), 2),
            "stdev": round(statistics.stdev(unc_prices), 2),
            "count": len(unc_prices),
            "note": "Low information items"
        }

    with open(OUTPUT_STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_stats, f, indent=4)
        
    print(f"\n[OK] Estadísticas sincronizadas guardadas en '{OUTPUT_STATS_FILE}'")
    print(f"    Categorías detectadas: {list(final_stats.keys())}")

if __name__ == "__main__":
    list_of_files = glob.glob('wallapop_raw_data_*.json') 
    if not list_of_files and os.path.exists("ejemplo_datos.txt"): 
        print("[!] No se encontraron archivos raw con patrón, usando 'ejemplo_datos.txt'")
        process_data("ejemplo_datos.txt")
    elif list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        process_data(latest_file)
    else:
        print("[!] No se encontraron archivos de datos.")