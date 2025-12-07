import json
import statistics
import spacy
import re
import glob
import os
from spacy.pipeline import EntityRuler
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Any

# --- CONFIGURACIÓN ---
OUTPUT_STATS_FILE = "market_stats_full.json"

# --- PALABRAS CLAVE PARA CLASIFICACIÓN ---

# 1. Rotos / Para piezas (Mercado Secundario)
BROKEN_KEYWORDS = [
    "roto", "averiado", "fallo", "bloqueado", "icloud", "bios", "pantalla rota", 
    "no enciende", "no funciona", "para piezas", "despiece", "repuesto", "tarada", 
    "golpe", "mojado", "water", "broken", "parts", "read", "leer", "reparar"
]

# 2. Accesorios INEQUÍVOCOS (Mercado Secundario)
ACCESSORY_KEYWORDS = [
    "funda", "carcasa", "maletin", "mochila", "bag", "sleeve", "skin",
    "dock", "docking", "hub", "base", "soporte", "stand", "cooler", "ventilador",
    "caja", "embalaje", "box", "vacia",
    "raton", "mouse", "alfombrilla", 
    "stylus", "lapiz", "pen",
    "protector", "cristal", "pegatina"
]

# 3. Indicadores Fuertes de Portátil (Mercado Primario)
LAPTOP_INDICATORS = [
    "portatil", "laptop", "macbook", "ordenador", "pc", "computer", 
    "notebook", "netbook", "ultrabook", "convertible", "2 en 1", "2 in 1",
    "surface pro", "thinkpad", "latitude", "precision", "xps", "inspiron",
    "zenbook", "vivobook", "rog", "tuf", "zephyrus", "legion", "ideapad", "yoga",
    "pavilion", "omen", "victus", "envy", "spectre", "elitebook", "probook",
    "matebook", "magicbook", "galaxy book", "prestige", "modern", "katana", "cyborg"
]

# --- PATRONES SPACY (HARDWARE) ---
PATTERNS = [
    # RAM (Estricta)
    {"label": "RAM", "pattern": [{"TEXT": {"REGEX": r"^\d+$"}}, {"LOWER": {"IN": ["gb", "giga", "gigas"]}}]},
    {"label": "RAM", "pattern": [{"TEXT": {"REGEX": r"^\d+gb$"}}]},
    
    # STORAGE (Para excluir)
    {"label": "STORAGE", "pattern": [{"TEXT": {"REGEX": r"^\d+(gb|tb)$"}}, {"LOWER": {"IN": ["ssd", "hdd", "nvme"]}}]},
    
    # CPU
    {"label": "CPU_BRAND", "pattern": [{"LOWER": {"IN": ["intel", "amd", "apple"]}}]},
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^i[3579]$"}}]},
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^core-?i[3579]$"}}]},
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^i9$"}}]},
    {"label": "CPU_MODEL", "pattern": [{"LOWER": "ryzen"}, {"TEXT": {"REGEX": r"^[3579]$"}}]},
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^ryzen[3579]$"}}]},
    {"label": "CPU_APPLE", "pattern": [{"LOWER": {"REGEX": r"^m[123]$"}}]},
    
    # GPU
    {"label": "GPU_BRAND", "pattern": [{"LOWER": {"IN": ["nvidia", "amd"]}}]},
    {"label": "GPU_MODEL", "pattern": [{"LOWER": {"IN": ["rtx", "gtx", "rx"]}}, {"TEXT": {"REGEX": r"^\d{3,4}[a-z]*$"}}]},
    {"label": "GPU_MODEL", "pattern": [{"TEXT": {"REGEX": r"^(rtx|gtx)\d{3,4}[a-z]*$"}}]},
]

# Categorías Principales (Prime Market)
SUB_CATEGORIES_RULES = {
    "APPLE": ["macbook", "mac", "apple", "macos"],
    "WORKSTATION": ["thinkpad", "latitude", "precision", "zbook", "quadro", "elitebook", "probook"],
    "PREMIUM_ULTRABOOK": ["xps", "spectre", "zenbook", "gram", "yoga", "matebook", "surface"],
    "GAMING": ["gaming", "gamer", "rog", "tuf", "alienware", "msi", "omen", "predator", "legion", "nitro", "victus", "loq", "blade", "razer"],
    "CHROMEBOOK": ["chromebook", "chrome"]
}

def load_spacy_model():
    print("[*] Cargando modelo spaCy...")
    try:
        nlp = spacy.load("es_core_news_sm")
    except OSError:
        print("[!] Modelo no encontrado. Instala: python -m spacy download es_core_news_sm")
        return None
    
    if "entity_ruler" in nlp.pipe_names: nlp.remove_pipe("entity_ruler")
    ruler = nlp.add_pipe("entity_ruler", before="ner")
    ruler.add_patterns(PATTERNS)
    return nlp

def clean_price(item):
    try:
        p = item.get("price", 0)
        if isinstance(p, dict): p = float(p.get("amount", 0))
        return float(p)
    except: return 0.0

def is_match(text_lower, keywords):
    for kw in keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
            return True
    return False

def determine_market_segment(title, description, price):
    title_lower = title.lower()
    
    if price < 20: return "JUNK"
    if price > 10000: return "JUNK"
    
    if is_match(title_lower, BROKEN_KEYWORDS):
        return "BROKEN"
        
    is_laptop = is_match(title_lower, LAPTOP_INDICATORS)
    is_accessory_keyword = is_match(title_lower, ACCESSORY_KEYWORDS)
    
    if is_accessory_keyword:
        if price < 100: return "ACCESSORY"
        if any(title_lower.startswith(x) for x in ["funda", "carcasa", "caja", "dock", "base"]):
            return "ACCESSORY"
        if is_laptop: return "PRIME"
        return "ACCESSORY"

    component_keywords = ["pantalla", "teclado", "bateria", "cargador", "placa base", "motherboard", "disco", "ssd", "ram"]
    if is_match(title_lower, component_keywords) and not is_laptop:
        return "ACCESSORY"

    return "PRIME"

def classify_prime_category(text_lower, specs):
    """
    Clasificación con reglas de negocio estrictas para evitar
    Apple con AMD o M2 en Gaming.
    """
    cpu_str = (specs.get("cpu") or "").upper()
    
    # 1. PRIORIDAD ABSOLUTA: Apple Silicon (M1, M2, M3)
    # Si tiene un procesador Apple, ES un Apple. No puede ser Gaming (aunque sirva para ello).
    if "APPLE M" in cpu_str:
        return "APPLE"

    # 2. Prioridad GPU (Gaming/Workstation)
    if specs.get("gpu"):
        if "quadro" in specs["gpu"].lower(): return "WORKSTATION"
        return "GAMING"
    
    # 3. Detección por Texto (Apple vs Resto)
    # "macbook" en título -> Apple
    if "apple" in specs.get("cpu_brand", "").lower() or "macbook" in text_lower or "macos" in text_lower:
        # REGLA DE ORO: Un Mac no puede tener procesador AMD Ryzen.
        # Si detectamos "AMD" en la CPU, asumimos que es un PC (ej: "Cambio PC Ryzen por Mac")
        if "AMD" in cpu_str:
            pass # No retornamos APPLE, dejamos que fluya a las reglas de PC
        else:
            return "APPLE"
        
    # 4. Resto de reglas por keywords
    for cat, kws in SUB_CATEGORIES_RULES.items():
        if cat in ["GAMING", "APPLE"]: continue
        if is_match(text_lower, kws): return cat
        
    if "gaming" in text_lower: return "GAMING"
    return "GENERICO"

def is_valid_ram(ram_text):
    try:
        val = int(re.sub(r"[^0-9]", "", ram_text))
        return val in [4, 6, 8, 12, 16, 20, 24, 32, 40, 48, 64]
    except: return False

def clean_cpu_string(brand, models, is_apple):
    models = sorted(list(models), reverse=True)
    if not models: return None
    best = models[0]
    
    if is_apple or "M" in best: brand = "APPLE"
    elif "RYZEN" in best: brand = "AMD"
    elif best.startswith("I") and len(best)>=2: brand = "INTEL"
    
    if "RYZEN" in best and best.replace("RYZEN", "")[0].isdigit():
         best = best.replace("RYZEN", "RYZEN ")

    return f"{brand} {best}".strip() if brand else best

def clean_gpu_string(brand, models):
    models = sorted(list(models), reverse=True)
    if not models: return None
    best = models[0]
    
    match = re.match(r"^([A-Z]+)(\d.*)$", best)
    if match and " " not in best: best = f"{match.group(1)} {match.group(2)}"
    
    if any(x in best for x in ["RTX", "GTX", "MX", "QUADRO"]): brand = "NVIDIA"
    elif any(x in best for x in ["RX", "RADEON", "FIREPRO"]): brand = "AMD"
    
    final = best.replace(brand or "", "").strip()
    return f"{brand} {final}".strip() if brand else final

def extract_specs(doc):
    specs = {"ram": None, "cpu_brand": None, "cpu_models": set(), "gpu_brand": None, "gpu_models": set(), "is_apple": False}
    
    for ent in doc.ents:
        lbl, txt = ent.label_, ent.text.upper().strip()
        
        if lbl == "RAM":
            clean = txt.replace(" ", "").replace("GIGAS", "GB").replace("GB", "") + "GB"
            if is_valid_ram(clean):
                if not specs["ram"]:
                    specs["ram"] = clean
                else:
                    try:
                        curr = int(re.sub(r"[^0-9]", "", specs["ram"]))
                        newv = int(re.sub(r"[^0-9]", "", clean))
                        if newv > curr: specs["ram"] = clean
                    except: pass
                    
        elif lbl == "CPU_BRAND": specs["cpu_brand"] = txt
        elif lbl == "CPU_MODEL": 
            norm = txt.replace("CORE", "").replace("-", "").strip()
            specs["cpu_models"].add(norm)
        elif lbl == "CPU_APPLE": 
            specs["cpu_models"].add(txt)
            specs["is_apple"] = True
        elif lbl == "GPU_BRAND": specs["gpu_brand"] = txt
        elif lbl == "GPU_MODEL": specs["gpu_models"].add(txt)

    has_pc_cpu = (specs["cpu_brand"] in ["INTEL", "AMD"]) or any((m.startswith("I") and m[1:].isdigit()) or "RYZEN" in m for m in specs["cpu_models"])
    if has_pc_cpu and specs["is_apple"]:
        specs["cpu_models"] = {m for m in specs["cpu_models"] if not re.match(r"^M[123]$", m)}
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

def process_data(input_file):
    nlp = load_spacy_model()
    if not nlp: return

    print(f"[*] Leyendo {input_file}...")
    with open(input_file, "r", encoding="utf-8") as f:
        items = json.load(f)

    market_data = {
        "PRIME": defaultdict(lambda: {"prices": [], "specs": {"cpu": defaultdict(list), "ram": defaultdict(list), "gpu": defaultdict(list)}}),
        "SECONDARY": defaultdict(list), 
        "UNCERTAIN": {"prices": []}     
    }

    texts = [f"{i.get('title', '')}. {i.get('description', '')}" for i in items]
    docs = nlp.pipe(texts, batch_size=100)
    
    print("[*] Clasificando y extrayendo datos...")
    
    for i, doc in enumerate(docs):
        item = items[i]
        price = clean_price(item)
        title = item.get('title', '')
        desc = item.get('description', '')
        
        segment = determine_market_segment(title, desc, price)
        
        if segment == "JUNK": continue
        
        if segment in ["BROKEN", "ACCESSORY"]:
            market_data["SECONDARY"][segment].append(price)
            continue
            
        specs = extract_specs(doc)
        
        if not specs["cpu"] and not specs["ram"]:
            market_data["UNCERTAIN"]["prices"].append(price)
            continue
            
        cat = classify_prime_category((title + " " + desc).lower(), specs)
        
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
        
    print(f"\n[OK] Estadísticas completas guardadas en '{OUTPUT_STATS_FILE}'")
    print(f"    Categorías detectadas: {list(final_stats.keys())}")

if __name__ == "__main__":
    import glob
    import os
    list_of_files = glob.glob('wallapop_raw_data_*.json') 
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        process_data(latest_file)
    else:
        print("[!] No se encontraron archivos raw.")