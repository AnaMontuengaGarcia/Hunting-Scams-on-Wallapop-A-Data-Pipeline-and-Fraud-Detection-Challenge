import json
import statistics
import spacy
import re
from spacy.pipeline import EntityRuler
from collections import defaultdict
from pathlib import Path
from typing import List, Dict, Any

# --- CONFIGURACIÓN ---
INPUT_FILE = "wallapop_raw_data_20231027_1000.json" # Ajusta al nombre real de tu archivo
OUTPUT_STATS_FILE = "market_stats_spacy.json"

# --- DEFINICIÓN DE ENTIDADES PARA SPACY (PATRONES) ---
PATTERNS = [
    # --- RAM ---
    # Patrones estrictos para evitar discos
    {"label": "RAM", "pattern": [{"TEXT": {"REGEX": r"^\d+$"}}, {"LOWER": {"IN": ["gb", "giga", "gigas", "gigabytes"]}}]},
    {"label": "RAM", "pattern": [{"TEXT": {"REGEX": r"^\d+gb$"}}]},

    # --- ALMACENAMIENTO (M.2 explícito para evitar confusión con Apple M2) ---
    {"label": "STORAGE", "pattern": [{"LOWER": "m.2"}]},
    {"label": "STORAGE", "pattern": [{"LOWER": "nvme"}]},
    {"label": "STORAGE", "pattern": [{"TEXT": {"REGEX": r"^\d+$"}}, {"LOWER": {"IN": ["tb", "tera", "teras"]}}]},
    {"label": "STORAGE", "pattern": [{"TEXT": {"REGEX": r"^\d+(gb|tb)$"}}, {"LOWER": {"IN": ["ssd", "hdd", "disco"]}}]},
    
    # --- CPU (Intel) ---
    {"label": "CPU_BRAND", "pattern": [{"LOWER": "intel"}]},
    # Detectamos i3, i5, i7, i9 aislados o con guion
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^i[3579]$"}}]}, 
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^core-?i[3579]$"}}]}, 
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^i9$"}}]}, 

    # --- CPU (AMD) ---
    {"label": "CPU_BRAND", "pattern": [{"LOWER": "amd"}]},
    {"label": "CPU_MODEL", "pattern": [{"LOWER": "ryzen"}, {"TEXT": {"REGEX": r"^[3579]$"}}]},
    {"label": "CPU_MODEL", "pattern": [{"LOWER": {"REGEX": r"^ryzen[3579]$"}}]},

    # --- CPU (Apple) ---
    # OJO: M1, M2, M3 pueden confundirse con almacenamiento M.2
    {"label": "CPU_APPLE", "pattern": [{"LOWER": {"REGEX": r"^m[123]$"}}]}, 
    {"label": "CPU_VARIANT", "pattern": [{"LOWER": {"IN": ["pro", "max", "ultra"]}}]},

    # --- GPU (Nvidia) ---
    {"label": "GPU_BRAND", "pattern": [{"LOWER": "nvidia"}]},
    {"label": "GPU_MODEL", "pattern": [{"LOWER": {"IN": ["rtx", "gtx"]}}, {"TEXT": {"REGEX": r"^\d{3,4}[a-z]*$"}}]},
    {"label": "GPU_MODEL", "pattern": [{"TEXT": {"REGEX": r"^(rtx|gtx)\d{3,4}[a-z]*$"}}]}, # rtx3060 junto

    # --- GPU (AMD) ---
    {"label": "GPU_BRAND", "pattern": [{"LOWER": "amd"}]}, # Añadido pattern de marca AMD explícito
    {"label": "GPU_MODEL", "pattern": [{"LOWER": "rx"}, {"TEXT": {"REGEX": r"^\d{3,4}[a-z]*$"}}]},
]

SUB_CATEGORIES_RULES = {
    "APPLE": ["macbook", "mac", "apple", "macos"],
    "WORKSTATION": ["thinkpad", "latitude", "precision", "zbook", "quadro", "elitebook", "probook"],
    "PREMIUM_ULTRABOOK": ["xps", "spectre", "zenbook", "gram", "yoga", "matebook", "galaxy", "envy", "swift"],
    "CHROMEBOOK": ["chromebook", "chrome"],
    "SURFACE": ["surface"],
    "GAMING": ["gaming", "gamer", "rog", "tuf", "alienware", "msi", "omen", "predator", "legion", "nitro", "victus", "blade", "razer", "loq"]
}

def load_spacy_model():
    print("[*] Cargando modelo spaCy (es_core_news_sm)...")
    try:
        nlp = spacy.load("es_core_news_sm")
    except OSError:
        print("[!] Modelo 'es_core_news_sm' no encontrado. Instálalo.")
        return None

    # Limpiamos pipelines anteriores si existieran
    if "entity_ruler" in nlp.pipe_names:
        nlp.remove_pipe("entity_ruler")

    ruler = nlp.add_pipe("entity_ruler", before="ner")
    ruler.add_patterns(PATTERNS)
    return nlp

def clean_price(item: Dict[str, Any]) -> float:
    try:
        price = item.get("price", 0)
        if isinstance(price, dict): price = float(price.get("amount", 0))
        return float(price)
    except: return 0.0

def classify_item(text: str, specs: Dict[str, str]) -> str:
    text_lower = text.lower()
    
    # 1. Prioridad GPU
    if specs.get("gpu"):
        if "quadro" in specs["gpu"].lower() or "firepro" in specs["gpu"].lower():
            return "WORKSTATION"
        return "GAMING"

    # 2. Prioridad Apple
    if "apple" in (specs.get("cpu") or "").lower() or "macbook" in text_lower:
        return "APPLE"

    # 3. Resto de reglas
    for cat, keywords in SUB_CATEGORIES_RULES.items():
        if cat in ["GAMING", "APPLE"]: continue
        for kw in keywords:
            if kw in text_lower: return cat
            
    if "gaming" in text_lower or "gamer" in text_lower:
        return "GAMING"
    return "GENERICO"

def is_valid_ram(ram_text: str) -> bool:
    """Filtro estricto para RAM de portátiles."""
    try:
        val = int(re.sub(r"[^0-9]", "", ram_text))
        # Lista blanca muy estricta. 
        # Eliminado 128GB porque en Wallapop el 99% de las veces es un SSD de 128GB mal etiquetado.
        return val in [4, 6, 8, 12, 16, 20, 24, 32, 48, 64]
    except ValueError:
        return False

def clean_cpu_string(brand: str, models: set, is_apple: bool) -> str:
    """Normaliza y limpia el nombre de la CPU."""
    # Convertimos set a lista y ordenamos para consistencia inversa (I7 antes que I5)
    models_list = sorted(list(models), reverse=True)
    if not models_list:
        return None
        
    # ESTRATEGIA: UN SOLO REY
    # Si hay "I7" y "I5", nos quedamos solo con "I7".
    best_model = models_list[0] 

    # Corrección Autoritaria de Marca según Modelo
    # Esto arregla "AMD I7" -> "INTEL I7" y "INTEL RYZEN" -> "AMD RYZEN"
    if "RYZEN" in best_model:
        brand = "AMD"
    elif best_model.startswith("I") and len(best_model) >= 2 and best_model[1].isdigit(): # I3, I5, I7, I9
        brand = "INTEL"
    elif is_apple:
        brand = "APPLE"
        
    return f"{brand} {best_model}".strip()

def clean_gpu_string(brand: str, models: set) -> str:
    """Normaliza y unifica el nombre de la GPU para evitar duplicados."""
    # Convertimos set a lista y ordenamos
    models_list = sorted(list(models), reverse=True)
    if not models_list:
        return None
        
    # Nos quedamos con el mejor modelo detectado
    best_model = models_list[0]
    
    # 1. Normalización de formato (separar letras de números si están pegados)
    # Ej: "RTX3060" -> "RTX 3060"
    match = re.match(r"^([A-Z]+)(\d.*)$", best_model)
    if match and " " not in best_model:
        prefix, number = match.groups()
        if prefix in ["RTX", "GTX", "RX", "GT", "MX"]:
            best_model = f"{prefix} {number}"

    # 2. Inferencia de Marca (Dictadura de la GPU)
    # Si el modelo es RTX/GTX/MX/GT -> Es NVIDIA sí o sí.
    if any(x in best_model for x in ["RTX", "GTX", "MX", "GT", "QUADRO"]):
        brand = "NVIDIA"
    elif any(x in best_model for x in ["RX", "RADEON", "FIREPRO"]):
        brand = "AMD"
    
    # 3. Construcción del string final unificado
    if brand:
        # Quitamos la marca del modelo si ya estaba ahí para evitar "NVIDIA NVIDIA RTX..."
        clean_model = best_model.replace(brand, "").strip()
        return f"{brand} {clean_model}"
    
    return best_model

def extract_specs_spacy(doc) -> Dict[str, str]:
    specs = {
        "ram": None,
        "cpu_brand": None,
        "cpu_models": set(),
        "gpu_brand": None,
        "gpu_models": set(),
        "is_apple_cpu": False
    }
    
    # Fase 1: Extracción cruda
    for ent in doc.ents:
        label = ent.label_
        text = ent.text.upper().strip()
        
        if label == "RAM":
            clean = text.replace(" ", "").replace("GIGAS", "GB").replace("GIGABYTES", "GB")
            if "GB" not in clean: clean += "GB"
            if is_valid_ram(clean):
                # Nos quedamos con la RAM más grande encontrada
                if not specs["ram"]:
                    specs["ram"] = clean
                else:
                    try:
                        curr = int(re.sub(r"[^0-9]", "", specs["ram"]))
                        new_val = int(re.sub(r"[^0-9]", "", clean))
                        if new_val > curr:
                            specs["ram"] = clean
                    except: pass
        
        elif label == "CPU_BRAND":
            specs["cpu_brand"] = text
        elif label == "CPU_MODEL":
            # Normalizamos "CORE-I7" a "I7"
            norm_model = text.replace("CORE-", "").replace("CORE", "").strip()
            
            # NORMALIZACIÓN RYZEN: "RYZEN5" -> "RYZEN 5"
            # Si empieza por RYZEN y le sigue un dígito inmediatamente, añadimos espacio
            if norm_model.startswith("RYZEN") and len(norm_model) > 5 and norm_model[5].isdigit():
                 norm_model = norm_model.replace("RYZEN", "RYZEN ")

            specs["cpu_models"].add(norm_model)
        elif label == "CPU_APPLE":
            specs["cpu_models"].add(text)
            specs["is_apple_cpu"] = True
            
        elif label == "GPU_BRAND":
            specs["gpu_brand"] = text
        elif label == "GPU_MODEL":
            specs["gpu_models"].add(text)

    # Fase 2: Resolución de Conflictos (CRÍTICO)
    
    # ¿Hay indicios de que es un PC (Intel/AMD)?
    has_pc_cpu = (specs["cpu_brand"] in ["INTEL", "AMD"]) or \
                 any((m.startswith("I") and m[1:].isdigit()) or "RYZEN" in m for m in specs["cpu_models"])

    if has_pc_cpu and specs["is_apple_cpu"]:
        # Si parece un PC, cualquier "M1/M2/M3" detectado es casi seguro un disco duro M.2
        # PURGAMOS los modelos de Apple
        specs["cpu_models"] = {m for m in specs["cpu_models"] if not re.match(r"^M[123]$", m)}
        specs["is_apple_cpu"] = False

    # Si es Apple legítimo, purgamos basura de PC
    if specs["is_apple_cpu"]:
        specs["cpu_brand"] = "APPLE"
        specs["cpu_models"] = {m for m in specs["cpu_models"] if re.match(r"^M[123]", m)}

    # Fase 3: Construcción
    final_specs = {"cpu": None, "ram": specs["ram"], "gpu": None}
    
    # CPU
    final_specs["cpu"] = clean_cpu_string(specs["cpu_brand"] or "", specs["cpu_models"], specs["is_apple_cpu"])
        
    # GPU
    final_specs["gpu"] = clean_gpu_string(specs["gpu_brand"] or "", specs["gpu_models"])
        
    return final_specs

def process_data_with_spacy(input_file: str):
    nlp = load_spacy_model()
    if not nlp: return

    path = Path(input_file)
    if not path.exists():
        print(f"[!] Archivo {input_file} no encontrado.")
        return

    print(f"[*] Leyendo datos RAW de {input_file}...")
    with open(path, "r", encoding="utf-8") as f:
        raw_items = json.load(f)

    print(f"[*] Procesando {len(raw_items)} anuncios con NLP...")
    
    categorized_data = defaultdict(lambda: {
        "prices": [],
        "specs_breakdown": {
            "cpu": defaultdict(list),
            "ram": defaultdict(list),
            "gpu": defaultdict(list)
        }
    })

    texts = [f"{item.get('title', '')}. {item.get('description', '')}" for item in raw_items]
    docs = nlp.pipe(texts, batch_size=50)
    
    for i, doc in enumerate(docs):
        item = raw_items[i]
        price = clean_price(item)
        if price < 20 or price > 8000: continue
        
        specs = extract_specs_spacy(doc)
        cat = classify_item(doc.text, specs)
        
        categorized_data[cat]["prices"].append(price)
        if specs["cpu"]: categorized_data[cat]["specs_breakdown"]["cpu"][specs["cpu"]].append(price)
        if specs["ram"]: categorized_data[cat]["specs_breakdown"]["ram"][specs["ram"]].append(price)
        if specs["gpu"]: categorized_data[cat]["specs_breakdown"]["gpu"][specs["gpu"]].append(price)

    stats = {}
    print("\n--- ESTADÍSTICAS GENERADAS CON NLP (SPACY) ---")
    
    for category, data in categorized_data.items():
        prices = data["prices"]
        if len(prices) < 5: continue
        
        mean = statistics.mean(prices)
        median = statistics.median(prices)
        stdev = statistics.stdev(prices) if len(prices) > 1 else 0
        
        stats[category] = {
            "mean": round(mean, 2),
            "median": round(median, 2),
            "stdev": round(stdev, 2),
            "sample_size": len(prices),
            "specs_breakdown": {
                "cpu": {}, "ram": {}, "gpu": {}
            }
        }
        
        for spec_type, spec_dict in data["specs_breakdown"].items():
            for spec_name, spec_prices in spec_dict.items():
                if len(spec_prices) >= 3:
                    stdev_spec = statistics.stdev(spec_prices) if len(spec_prices) > 1 else 0
                    stats[category]["specs_breakdown"][spec_type][spec_name] = {
                        "mean": round(statistics.mean(spec_prices), 2),
                        "median": round(statistics.median(spec_prices), 2),
                        "stdev": round(stdev_spec, 2),
                        "count": len(spec_prices)
                    }
        
        print(f"[{category}] Muestra: {len(prices)} | Media: {mean:.0f}€ | Mediana: {median:.0f}€")

    with open(OUTPUT_STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats, f, indent=4)
    print(f"\n[*] Análisis completado. Estadísticas guardadas en '{OUTPUT_STATS_FILE}'")

if __name__ == "__main__":
    import glob
    import os
    list_of_files = glob.glob('wallapop_raw_data_*.json') 
    if list_of_files:
        latest_file = max(list_of_files, key=os.path.getctime)
        process_data_with_spacy(latest_file)
    else:
        print("[!] No se encontraron archivos 'wallapop_raw_data_*.json'.")