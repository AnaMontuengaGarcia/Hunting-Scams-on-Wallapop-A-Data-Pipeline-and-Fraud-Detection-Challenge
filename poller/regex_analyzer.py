#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Módulo: regex_analyzer.py
Descripción: Motor de análisis de especificaciones hardware y generación de
             estadísticas de mercado mediante expresiones regulares.

Funcionalidades principales:
    - Extracción de especificaciones hardware (CPU, RAM, GPU) de texto
    - Clasificación de productos en categorías (Gaming, Apple, Workstation...)
    - Detección de condición del producto (Nuevo, Usado, Roto...)
    - Detección de precios ocultos en descripciones
    - Generación de estadísticas de mercado segmentadas

Arquitectura del análisis:
    1. PREPROCESAMIENTO: Limpieza y sanitización del texto
    2. EXTRACCIÓN: Patrones regex para identificar componentes
    3. CLASIFICACIÓN: Reglas para categorizar el tipo de equipo
    4. AGREGACIÓN: Cálculo de estadísticas por segmento

Dependencias:
    - re: Motor de expresiones regulares estándar
    - statistics: Cálculos estadísticos (media, mediana, stdev)
    - json: Serialización de resultados

Uso:
    # Como módulo importado:
    from regex_analyzer import get_prioritized_specs_and_category
    specs, category, condition = get_prioritized_specs_and_category(title, desc)
    
    # Como script independiente:
    python regex_analyzer.py  # Procesa el archivo JSON más reciente
================================================================================
"""

import json
import statistics
import re
import glob
import os
from collections import defaultdict
from typing import List, Dict, Set, Any, Tuple, Optional


# =============================================================================
# CONSTANTES DE CONFIGURACIÓN
# =============================================================================

# Archivo de salida para las estadísticas de mercado generadas
OUTPUT_STATS_FILE = "market_stats.json"

# Límites máximos de RAM por categoría de dispositivo
# Previene falsos positivos cuando se detectan valores absurdos
RAM_LIMITS = {
    "CHROMEBOOK": 16,          # Chromebooks raramente superan 16GB
    "SURFACE": 32,             # Surface Pro máximo 32GB
    "PREMIUM_ULTRABOOK": 64,   # Ultrabooks premium hasta 64GB
    "GENERICO": 64             # Límite genérico para portátiles
}


# =============================================================================
# EXPRESIONES REGULARES - DETECCIÓN DE PRECIOS
# =============================================================================

# Patrón para detectar precios ocultos en el texto
# Ejemplo: "precio: 450€", "vendo por 300 euros"
RE_HIDDEN_PRICE = re.compile(
    r'(?i)(?:precio|valor|vende|vendo|pido|oferta)[:\s]*(?:por)?\s*(\d{2,4})(?:[\.,]\d{2})?\s*(?:€|eur|euros)',
    re.IGNORECASE
)

# Patrón más permisivo para precios sueltos
# Ejemplo: "450€", "300 euros"
RE_LOOSE_PRICE = re.compile(r'\b(\d{2,4})\s*(?:€|euros)\b', re.IGNORECASE)


# =============================================================================
# EXPRESIONES REGULARES - DETECCIÓN DE CONDICIÓN
# =============================================================================

# Indicadores de producto NUEVO
RE_CONDITION_NEW = re.compile(
    r'\b(nuevo|precintado|sin abrir|estrenar|sealed|new|garantia|factura)\b',
    re.IGNORECASE
)

# Indicadores de producto COMO NUEVO
RE_CONDITION_LIKE_NEW = re.compile(
    r'\b(como nuevo|impecable|perfecto estado|reacondicionado|refurbished|poquisimo uso|sin uso)\b',
    re.IGNORECASE
)

# Indicadores de producto DEFECTUOSO/PARA PIEZAS
RE_CONDITION_BROKEN = re.compile(
    r'\b(roto|averiado|fallo|bloqueado|icloud|bios|pantalla rota|no enciende|no funciona|para piezas|despiece|repuesto|tarada|golpe|mojado|water|broken|parts|read|leer|reparar)\b',
    re.IGNORECASE
)


# =============================================================================
# EXPRESIONES REGULARES - DETECCIÓN DE HARDWARE
# =============================================================================

# Patrón para RAM (evita confusión con almacenamiento)
# El negative lookahead excluye "GB SSD", "GB HDD", etc.
RE_RAM = re.compile(
    r'\b(\d+)\s*(?:gb|gigas?)\b(?!\s*(?:[\.,\-\/]\s*)?(?:de\s+)?(?:ssd|hdd|emmc|rom|almacenamiento|storage|disco|nvme|flash|interno|interna))',
    re.IGNORECASE
)

# Marcas de procesadores
RE_CPU_BRAND = re.compile(r'\b(intel|amd|apple|qualcomm|microsoft)\b', re.IGNORECASE)

# Modelos específicos de procesadores
RE_CPU_MODELS = [
    re.compile(r'\b(core\s*-?)?(i[3579])\b', re.IGNORECASE),       # Intel Core i3/i5/i7/i9
    re.compile(r'\b(ryzen)\s*-?([3579])\b', re.IGNORECASE),        # AMD Ryzen 3/5/7/9
    re.compile(r'\b(m[123])\s*(pro|max|ultra)?\b', re.IGNORECASE), # Apple M1/M2/M3
    re.compile(r'\b(celeron|pentium|atom|xeon)\b', re.IGNORECASE), # Intel gama baja/servidor
    re.compile(r'\b(snapdragon|sq[123])\b', re.IGNORECASE)         # Qualcomm/Microsoft ARM
]

# Marcas de tarjetas gráficas
RE_GPU_BRAND = re.compile(r'\b(nvidia|amd|radeon|geforce)\b', re.IGNORECASE)

# Modelos de tarjetas gráficas dedicadas
RE_GPU_MODEL = re.compile(r'\b((?:rtx|gtx|rx)\s*-?\d{3,4}[a-z]*)\b', re.IGNORECASE)


# =============================================================================
# REGLAS DE CLASIFICACIÓN DE CATEGORÍAS
# =============================================================================

# Palabras clave para clasificar el tipo de portátil
SUB_CATEGORIES_RULES = {
    "APPLE": ["macbook", "mac", "apple", "macos"],
    "SURFACE": ["surface", "microsoft surface"],
    "WORKSTATION": ["thinkpad", "latitude", "precision", "zbook", "quadro", "elitebook", "probook"],
    "PREMIUM_ULTRABOOK": ["xps", "spectre", "zenbook", "gram", "yoga", "matebook"],
    "GAMING": ["gaming", "gamer", "rog", "tuf", "alienware", "msi", "omen", "predator", "legion", "nitro", "victus", "loq", "blade", "razer"],
    "CHROMEBOOK": ["chromebook", "chrome"]
}


# =============================================================================
# FUNCIONES DE UTILIDAD - EXTRACCIÓN DE PRECIOS
# =============================================================================

def clean_price(item: Dict) -> float:
    """
    Extrae y normaliza el precio de un artículo.
    
    Maneja diferentes formatos de precio que puede devolver la API:
    - Número directo: {"price": 450}
    - Objeto con amount: {"price": {"amount": 450, "currency": "EUR"}}
    
    Args:
        item (Dict): Diccionario con datos del artículo.
    
    Returns:
        float: Precio normalizado, o 0.0 si no se puede extraer.
    """
    try:
        p = item.get("price", 0)
        if isinstance(p, dict):
            p = float(p.get("amount", 0))
        return float(p)
    except:
        return 0.0


def try_extract_hidden_price(title: str, description: str) -> Optional[float]:
    """
    Intenta extraer un precio real oculto en el texto del anuncio.
    
    Algunos vendedores ponen precio simbólico (1€, 0€) y escriben
    el precio real en la descripción. Esta función busca patrones
    comunes de precios ocultos.
    
    Args:
        title (str): Título del anuncio.
        description (str): Descripción del producto.
    
    Returns:
        Optional[float]: Precio encontrado, o None si no se detecta.
    
    Estrategia de búsqueda:
        1. Buscar patrones estructurados ("precio: X€")
        2. Si no hay, buscar menciones sueltas de precios
        3. Filtrar rangos razonables (50-5000€)
        4. En caso de múltiples, devolver el mayor (asumido como precio real)
    """
    full_text = f"{title} \n {description}"
    
    # Primera pasada: patrones estructurados
    matches = RE_HIDDEN_PRICE.findall(full_text)
    for m in matches:
        try:
            val = float(m)
            if val > 20:  # Filtrar precios simbólicos
                return val
        except:
            pass
    
    # Segunda pasada: menciones sueltas
    matches_loose = RE_LOOSE_PRICE.findall(full_text)
    candidates = []
    for m in matches_loose:
        try:
            val = float(m)
            if 50 <= val <= 5000:  # Rango razonable para portátiles
                candidates.append(val)
        except:
            pass
    
    if candidates:
        return max(candidates)  # Devolver el precio más alto
    
    return None


# =============================================================================
# FUNCIONES DE UTILIDAD - PROCESAMIENTO DE TEXTO
# =============================================================================

def is_match(text_lower: str, keywords: List[str]) -> bool:
    """
    Verifica si alguna palabra clave aparece en el texto.
    
    Usa búsqueda de palabras completas para evitar falsos positivos
    (ej: "gaming" no debería coincidir con "imagining").
    
    Args:
        text_lower (str): Texto en minúsculas a analizar.
        keywords (List[str]): Lista de palabras clave a buscar.
    
    Returns:
        bool: True si alguna palabra clave está presente.
    """
    for kw in keywords:
        if re.search(r'\b' + re.escape(kw) + r'\b', text_lower):
            return True
    return False


def smart_truncate_spam(text: str) -> str:
    """
    Trunca descripciones con contenido spam repetitivo.
    
    Algunos vendedores incluyen listas extensas de productos no
    relacionados para mejorar su visibilidad en búsquedas. Esta
    función detecta y elimina ese contenido irrelevante.
    
    Args:
        text (str): Texto de la descripción a procesar.
    
    Returns:
        str: Texto limpio sin contenido spam.
    
    Criterio de detección:
        Si una línea contiene más de 3 indicadores de spam
        (marcas, modelos de otros productos), se trunca el resto.
    """
    lines = text.split('\n')
    clean_lines = []
    
    # Indicadores de spam (menciones a productos no relacionados)
    spam_indicators = [
        "rtx", "gtx", "amd", "intel", "ryzen", "i7", "i5",
        "ps5", "xbox", "iphone", "samsung", "asus", "msi"
    ]
    
    for line in lines:
        hits = 0
        line_lower = line.lower()
        
        for ind in spam_indicators:
            if ind in line_lower:
                hits += 1
        
        # Si hay demasiadas menciones, probablemente es spam
        if hits > 3:
            break
        
        clean_lines.append(line)
    
    return "\n".join(clean_lines)


def sanitize_hardware_ambiguities(text: str) -> str:
    """
    Resuelve ambigüedades en nomenclaturas de hardware.
    
    El término "M.2" puede confundirse con los procesadores Apple M2.
    Esta función modifica el texto para evitar falsos positivos.
    
    Args:
        text (str): Texto a sanitizar.
    
    Returns:
        str: Texto con ambigüedades resueltas.
    
    Transformaciones:
        - "SSD M.2" -> "SSD_NVME"
        - "M.2 SSD" -> "NVME_SSD"
    """
    # M.2 como tipo de SSD, no como procesador Apple
    text = re.sub(r"(?i)\b(ssd|disco|disk|drive|almacenamiento)\s+m\.?2\b", r"\1_NVME", text)
    text = re.sub(r"(?i)\bm\.?2\s+(ssd|nvme|sata)\b", r"NVME_\1", text)
    
    return text


# =============================================================================
# FUNCIONES DE DETECCIÓN DE CONDICIÓN
# =============================================================================

def detect_condition_from_data(item: Dict, full_text_lower: str) -> str:
    """
    Determina la condición del producto usando múltiples fuentes.
    
    Implementa una estrategia de fallback que prioriza datos
    estructurados de la API sobre análisis de texto.
    
    Args:
        item (Dict): Datos del artículo (puede incluir campos de API).
        full_text_lower (str): Texto completo del anuncio en minúsculas.
    
    Returns:
        str: Condición detectada (NEW, LIKE_NEW, USED, BROKEN).
    
    Orden de prioridad:
        1. Campo type_attributes.condition de la API (más fiable)
        2. Flag is_refurbished de la API
        3. Análisis de texto con expresiones regulares
    """
    # 1. Datos estructurados de la API (más fiable)
    api_cond = None
    try:
        api_cond = item.get("type_attributes", {}).get("condition", {}).get("value")
    except AttributeError:
        pass  # type_attributes podría ser None

    if api_cond:
        if api_cond == "new":
            return "NEW"
        if api_cond == "as_good_as_new":
            return "LIKE_NEW"
        if api_cond == "has_given_it_all":
            return "BROKEN"
        # "good", "fair" -> USED
        return "USED"

    # 2. Flag de producto reacondicionado
    is_refurbished_data = item.get("is_refurbished")
    if is_refurbished_data and isinstance(is_refurbished_data, dict) and is_refurbished_data.get("flag") is True:
        return "LIKE_NEW"

    # 3. Fallback: Análisis de texto con regex
    if RE_CONDITION_BROKEN.search(full_text_lower):
        return "BROKEN"
    if RE_CONDITION_NEW.search(full_text_lower):
        return "NEW"
    if RE_CONDITION_LIKE_NEW.search(full_text_lower):
        return "LIKE_NEW"
    
    return "USED"


# =============================================================================
# FUNCIONES DE VALIDACIÓN Y CORRECCIÓN
# =============================================================================

def apply_category_constraints(specs: Dict, category: str, full_text_original: str) -> Dict:
    """
    Aplica restricciones lógicas a las especificaciones según la categoría.
    
    Corrige detecciones implausibles basándose en límites conocidos
    por tipo de dispositivo (ej: Chromebook con 128GB RAM es imposible).
    
    Args:
        specs (Dict): Especificaciones detectadas (cpu, ram, gpu).
        category (str): Categoría del dispositivo.
        full_text_original (str): Texto original para re-análisis.
    
    Returns:
        Dict: Especificaciones corregidas.
    
    Correcciones aplicadas:
        - RAM superior al límite de la categoría -> Re-extraer
        - Chromebook con i7 detectado -> Verificar Celeron/Pentium
    """
    # Obtener límite de RAM para esta categoría
    limit_ram = RAM_LIMITS.get(category, 128)
    
    # Verificar si la RAM detectada excede el límite
    current_ram_gb = 0
    if specs.get("ram"):
        try:
            current_ram_gb = int(re.sub(r"[^0-9]", "", specs["ram"]))
        except:
            pass
    
    # Corregir RAM si excede el límite
    if current_ram_gb > limit_ram:
        corrected_ram = extract_ram(full_text_original.lower(), max_gb=limit_ram)
        specs["ram"] = corrected_ram if corrected_ram else None

    # Corrección específica para Chromebooks
    # Los Chromebooks con "i7" detectado suelen ser falsos positivos
    if category == "CHROMEBOOK" and specs.get("cpu") and "I7" in specs["cpu"]:
        if "celeron" in full_text_original.lower():
            specs["cpu"] = "INTEL CELERON"
        elif "pentium" in full_text_original.lower():
            specs["cpu"] = "INTEL PENTIUM"
             
    return specs


def is_valid_ram(ram_val: int) -> bool:
    """
    Valida si un valor de RAM es comercialmente plausible.
    
    Los módulos de RAM vienen en tamaños estándar. Esta función
    filtra valores improbables que suelen ser falsos positivos.
    
    Args:
        ram_val (int): Valor de RAM en GB a validar.
    
    Returns:
        bool: True si el valor es plausible para un portátil.
    
    Valores válidos:
        4, 6, 8, 12, 16, 20, 24, 32, 40, 48, 64 GB
    """
    return ram_val in [4, 6, 8, 12, 16, 20, 24, 32, 40, 48, 64]


# =============================================================================
# FUNCIONES DE NORMALIZACIÓN DE COMPONENTES
# =============================================================================

def clean_cpu_string(brand: str, models: Set[str], is_apple: bool) -> Optional[str]:
    """
    Normaliza la cadena de CPU a un formato estándar.
    
    Combina marca y modelo detectados en una cadena consistente
    para facilitar la agregación estadística.
    
    Args:
        brand (str): Marca del procesador detectada.
        models (Set[str]): Conjunto de modelos detectados.
        is_apple (bool): Flag indicando si se detectó procesador Apple.
    
    Returns:
        Optional[str]: Cadena normalizada (ej: "INTEL I7", "APPLE M2 PRO")
                      o None si no hay datos suficientes.
    
    Ejemplos de normalización:
        - {"i7"} + "intel" -> "INTEL I7"
        - {"m2", "pro"} + apple=True -> "APPLE M2 PRO"
        - {"ryzen", "7"} -> "AMD RYZEN 7"
    """
    models = sorted(list(models), reverse=True)
    if not models:
        return None
    
    best = models[0].upper()
    
    # Determinar marca basándose en el modelo
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
    
    # Formatear Ryzen con espacio
    if "RYZEN" in best and best.replace("RYZEN", "") and best.replace("RYZEN", "")[0].isdigit():
        best = best.replace("RYZEN", "RYZEN ")
    
    # Formatear Apple con prefijo
    if brand == "APPLE" and not best.startswith("APPLE"):
        return f"APPLE {best}"
    
    return f"{brand} {best}".strip() if brand else best


def clean_gpu_string(brand: str, models: Set[str]) -> Optional[str]:
    """
    Normaliza la cadena de GPU a un formato estándar.
    
    Similar a clean_cpu_string, pero para tarjetas gráficas.
    
    Args:
        brand (str): Marca de la GPU detectada.
        models (Set[str]): Conjunto de modelos detectados.
    
    Returns:
        Optional[str]: Cadena normalizada (ej: "NVIDIA RTX 4070")
                      o None si no hay datos suficientes.
    """
    models = sorted(list(models), reverse=True)
    if not models:
        return None
    
    best = models[0].upper()
    
    # Insertar espacio entre prefijo y número si no existe
    match = re.match(r"^([A-Z]+)(\d.*)$", best)
    if match and " " not in best:
        best = f"{match.group(1)} {match.group(2)}"
    
    # Determinar marca basándose en el modelo
    if any(x in best for x in ["RTX", "GTX", "MX", "QUADRO"]):
        brand = "NVIDIA"
    elif any(x in best for x in ["RX", "RADEON", "FIREPRO"]):
        brand = "AMD"
    
    # Evitar duplicación de marca
    final = best.replace(brand or "", "").strip()
    return f"{brand} {final}".strip() if brand else final


# =============================================================================
# FUNCIONES DE EXTRACCIÓN DE HARDWARE
# =============================================================================

def extract_ram(text_lower: str, max_gb: int = 128) -> Optional[str]:
    """
    Extrae el valor de RAM del texto.
    
    Busca menciones de RAM y selecciona el valor más alto
    que sea plausible y esté dentro del límite especificado.
    
    Args:
        text_lower (str): Texto en minúsculas a analizar.
        max_gb (int): Límite máximo de RAM a aceptar.
    
    Returns:
        Optional[str]: RAM formateada (ej: "16GB") o None.
    """
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
        except:
            pass
    
    return ram_str


def extract_specs_regex(text: str) -> Dict:
    """
    Extrae todas las especificaciones hardware de un texto.
    
    Aplica múltiples expresiones regulares para detectar
    CPU, RAM y GPU en el texto proporcionado.
    
    Args:
        text (str): Texto a analizar.
    
    Returns:
        Dict: Diccionario con especificaciones detectadas:
            - ram: Cadena de RAM (ej: "16GB")
            - cpu_brand: Marca del procesador
            - cpu_models: Set de modelos de CPU detectados
            - gpu_brand: Marca de la GPU
            - gpu_models: Set de modelos de GPU detectados
            - is_apple: Flag de procesador Apple
    """
    text_lower = text.lower()
    
    specs = {
        "ram": None,
        "cpu_brand": None,
        "cpu_models": set(),
        "gpu_brand": None,
        "gpu_models": set(),
        "is_apple": False
    }
    
    # Extraer RAM
    specs["ram"] = extract_ram(text_lower)
    
    # Extraer marca de CPU
    brand_matches = RE_CPU_BRAND.findall(text_lower)
    if brand_matches:
        specs["cpu_brand"] = brand_matches[0].upper()

    # Extraer modelos de CPU
    for pattern in RE_CPU_MODELS:
        matches = pattern.findall(text_lower)
        for m in matches:
            full_model = ""
            if isinstance(m, tuple):
                # Procesar grupos capturados en la expresión regular
                parts = [p for p in m if p]
                if parts[0].lower().startswith("m") and len(parts) > 1:
                    # Apple M1/M2/M3 con variante (Pro, Max, Ultra)
                    full_model = f"{parts[0]} {parts[1]}"
                else:
                    full_model = "".join(parts).replace(" ", "").replace("-", "")
            else:
                full_model = m.replace(" ", "")
            
            # Clasificar y normalizar el modelo detectado
            if "ryzen" in full_model.lower():
                specs["cpu_models"].add(f"RYZEN{re.sub(r'[^0-9]', '', full_model)}")
            elif full_model.lower().startswith("m") and full_model[1].isdigit():
                specs["cpu_models"].add(full_model.upper())
                specs["is_apple"] = True
            elif full_model.lower().startswith("i") and full_model[1].isdigit():
                specs["cpu_models"].add(full_model.upper())
            elif any(x in full_model.lower() for x in ["celeron", "pentium", "atom", "xeon", "snapdragon", "sq1", "sq2", "sq3"]):
                specs["cpu_models"].add(full_model.upper())

    # Extraer marca de GPU
    gpu_brand_matches = RE_GPU_BRAND.findall(text_lower)
    if gpu_brand_matches:
        specs["gpu_brand"] = gpu_brand_matches[0].upper()
        # Normalizar GeForce -> NVIDIA
        if specs["gpu_brand"] in ["GEFORCE"]:
            specs["gpu_brand"] = "NVIDIA"
    
    # Extraer modelos de GPU
    gpu_model_matches = RE_GPU_MODEL.findall(text_lower)
    for gm in gpu_model_matches:
        specs["gpu_models"].add(gm.upper())

    # Resolver conflictos: Si detectamos CPU Intel/AMD, no puede ser Apple
    has_pc_cpu = (specs["cpu_brand"] in ["INTEL", "AMD"]) or any(
        (m.startswith("I") and m[1:].isdigit()) or "RYZEN" in m
        for m in specs["cpu_models"]
    )
    
    if has_pc_cpu and specs["is_apple"]:
        # Eliminar procesadores Apple si hay CPU Intel/AMD detectada
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


# =============================================================================
# FUNCIONES DE CLASIFICACIÓN DE CATEGORÍAS
# =============================================================================

def classify_prime_category(text_lower: str, specs: Dict) -> str:
    """
    Clasifica un portátil en su categoría principal.
    
    Utiliza una combinación de detección de hardware y
    palabras clave para determinar el tipo de dispositivo.
    
    Args:
        text_lower (str): Texto del anuncio en minúsculas.
        specs (Dict): Especificaciones hardware detectadas.
    
    Returns:
        str: Categoría del dispositivo (APPLE, GAMING, WORKSTATION, etc.)
    
    Lógica de clasificación:
        1. Procesador Apple M -> APPLE
        2. GPU Quadro -> WORKSTATION
        3. Cualquier GPU dedicada -> GAMING
        4. Palabras clave de marca/modelo -> Categoría correspondiente
        5. Default -> GENERICO
    """
    cpu_str = (specs.get("cpu") or "").upper()
    
    # Procesador Apple = categoría Apple
    if "APPLE M" in cpu_str:
        return "APPLE"
    
    # GPU Quadro = Workstation profesional
    if specs.get("gpu") and "quadro" in specs["gpu"].lower():
        return "WORKSTATION"
    
    # Cualquier GPU dedicada = Gaming
    if specs.get("gpu"):
        return "GAMING"
    
    # Verificar indicadores de Apple sin procesador M detectado
    if "apple" in specs.get("cpu_brand", "").lower() or "macbook" in text_lower or "macos" in text_lower:
        if "AMD" not in cpu_str:
            return "APPLE"
    
    # Buscar por palabras clave de categoría
    for cat, kws in SUB_CATEGORIES_RULES.items():
        if cat in ["GAMING", "APPLE"]:
            continue  # Ya procesadas arriba
        if is_match(text_lower, kws):
            return cat
    
    # Mención explícita de gaming
    if "gaming" in text_lower:
        return "GAMING"
    
    return "GENERICO"


def get_prioritized_specs_and_category(title: str, description: str) -> Tuple[Dict, str, str]:
    """
    Función principal de análisis para uso en tiempo real.
    
    Combina extracción de specs, clasificación de categoría y
    detección de condición en una sola llamada optimizada.
    
    Args:
        title (str): Título del anuncio.
        description (str): Descripción del producto.
    
    Returns:
        Tuple[Dict, str, str]: Tupla con:
            - specs: Diccionario {cpu, ram, gpu}
            - category: Categoría del dispositivo
            - condition: Condición detectada por regex
    
    Notas:
        - El título tiene prioridad sobre la descripción
        - Solo analiza los primeros 400 chars de la descripción
        - Aplica sanitización de texto antes del análisis
    """
    # Preprocesamiento del texto
    title_clean = sanitize_hardware_ambiguities(title)
    desc_clean = sanitize_hardware_ambiguities(smart_truncate_spam(description))
    
    full_text = f"{title_clean} {desc_clean}".lower()
    
    # 1. Extracción de especificaciones
    # El título tiene prioridad, fallback a descripción
    specs_title = extract_specs_regex(title_clean)
    specs_desc = extract_specs_regex(desc_clean[:400])  # Limitar descripción
    
    final_specs = {
        "cpu": specs_title.get("cpu") if specs_title.get("cpu") else specs_desc.get("cpu"),
        "ram": specs_title.get("ram") if specs_title.get("ram") else specs_desc.get("ram"),
        "gpu": specs_title.get("gpu") if specs_title.get("gpu") else specs_desc.get("gpu"),
    }
    
    # 2. Clasificación de categoría
    cat = "GENERICO"
    if "chromebook" in title_clean.lower():
        cat = "CHROMEBOOK"
    elif any(x in title_clean.lower() for x in ["macbook", "mac air", "mac pro", "imac"]):
        cat = "APPLE"
    elif "surface" in title_clean.lower():
        cat = "SURFACE"
    else:
        cat = classify_prime_category(full_text, final_specs)
    
    # Aplicar restricciones de categoría
    final_specs = apply_category_constraints(final_specs, cat, full_text)
    
    # 3. Detección de condición (solo regex, sin datos de API)
    cond = "USED"
    if RE_CONDITION_BROKEN.search(full_text):
        cond = "BROKEN"
    elif RE_CONDITION_NEW.search(full_text):
        cond = "NEW"
    elif RE_CONDITION_LIKE_NEW.search(full_text):
        cond = "LIKE_NEW"
    
    return final_specs, cat, cond


# =============================================================================
# FUNCIONES DE SEGMENTACIÓN DE MERCADO
# =============================================================================

def determine_market_segment(title_lower: str, price: float, condition: str, specs: Dict) -> str:
    """
    Determina el segmento de mercado de un artículo.
    
    Clasifica los artículos en segmentos para facilitar
    el análisis estadístico separado.
    
    Args:
        title_lower (str): Título en minúsculas.
        price (float): Precio del artículo.
        condition (str): Condición del producto.
        specs (Dict): Especificaciones detectadas.
    
    Returns:
        str: Segmento de mercado:
            - UNCERTAIN: Precio simbólico, no analizable
            - JUNK: Precio absurdo (>10,000€)
            - BROKEN: Producto defectuoso
            - ACCESSORY: Accesorio, no portátil completo
            - PRIME: Portátil completo analizable
    """
    # Precios fuera de rango razonable
    if price < 5:
        return "UNCERTAIN"
    if price > 10000:
        return "JUNK"
    
    # Productos defectuosos tienen su propio segmento
    if condition == "BROKEN":
        return "BROKEN"
    
    # Detectar si es un portátil completo
    is_laptop = False
    for ind in ["portatil", "laptop", "macbook"]:
        if ind in title_lower:
            is_laptop = True

    # Detectar accesorios
    is_accessory = False
    for kw in ["funda", "caja", "dock", "raton"]:
        if kw in title_lower:
            is_accessory = True
    
    # Accesorios baratos o productos que no son portátiles
    if is_accessory and price < 100:
        return "ACCESSORY"
    if is_accessory and not is_laptop:
        return "ACCESSORY"
    
    return "PRIME"


# =============================================================================
# FUNCIÓN PRINCIPAL DE GENERACIÓN DE ESTADÍSTICAS
# =============================================================================

def process_data(input_file: str) -> None:
    """
    Procesa un archivo de datos raw y genera estadísticas de mercado.
    
    Lee un archivo JSON con artículos recolectados, analiza cada uno
    para extraer especificaciones y condición, y genera estadísticas
    agregadas por categoría, condición y componente.
    
    Args:
        input_file (str): Ruta al archivo JSON de entrada.
    
    Returns:
        None
    
    Archivo generado:
        market_stats.json con estructura:
        {
            "CATEGORY": {
                "CONDITION": {
                    "mean": float,
                    "median": float,
                    "stdev": float,
                    "count": int,
                    "components": {
                        "cpu": {"MODEL": {stats}},
                        "ram": {"16GB": {stats}},
                        "gpu": {"MODEL": {stats}}
                    }
                }
            }
        }
    
    Flujo de procesamiento:
        1. Leer archivo JSON de entrada
        2. Para cada artículo:
           a. Extraer especificaciones y categoría
           b. Detectar condición del producto
           c. Determinar segmento de mercado
           d. Agregar precio a las estadísticas correspondientes
        3. Calcular estadísticas (media, mediana, stdev)
        4. Guardar en archivo JSON
    """
    print(f"[*] Generando Estadísticas (Anidadas por Estado: NEW/USED/etc) desde {input_file}...")
    
    with open(input_file, "r", encoding="utf-8") as f:
        items = json.load(f)

    # Estructura de datos para agregación
    # CATEGORY -> CONDITION -> SPECS -> [Prices]
    market_data = {
        "PRIME": defaultdict(
            lambda: defaultdict(
                lambda: {
                    "prices": [],
                    "specs": {
                        "cpu": defaultdict(list),
                        "ram": defaultdict(list),
                        "gpu": defaultdict(list)
                    }
                }
            )
        ),
        "SECONDARY": defaultdict(list),
        "UNCERTAIN": {"prices": []}
    }

    count = 0
    
    # =========================================================================
    # BUCLE DE PROCESAMIENTO DE ARTÍCULOS
    # =========================================================================
    for item in items:
        price = clean_price(item)
        title = item.get('title', '') or ""
        desc = item.get('description', '') or ""
        
        # 1. Extracción de especificaciones y categoría base
        specs, cat, _ = get_prioritized_specs_and_category(title, desc)
        
        # 2. Detección inteligente de condición (API > Regex)
        full_text = (title + " " + desc).lower()
        condition = detect_condition_from_data(item, full_text)
        
        # 3. Segmentación de mercado
        segment = determine_market_segment(title.lower(), price, condition, specs)
        
        # Filtrar segmentos no analizables
        if segment == "JUNK":
            continue
        
        if segment == "UNCERTAIN" or (not specs["cpu"] and not specs["ram"]):
            market_data["UNCERTAIN"]["prices"].append(price)
            continue
        
        if segment in ["BROKEN", "ACCESSORY"]:
            if segment == "BROKEN":
                market_data["SECONDARY"]["BROKEN"].append(price)
            else:
                market_data["SECONDARY"]["ACCESSORY"].append(price)
            continue
        
        # Agregar a estadísticas PRIME (segmento principal)
        group = market_data["PRIME"][cat][condition]
        group["prices"].append(price)
        
        if specs["cpu"]:
            group["specs"]["cpu"][specs["cpu"]].append(price)
        if specs["ram"]:
            group["specs"]["ram"][specs["ram"]].append(price)
        if specs["gpu"]:
            group["specs"]["gpu"][specs["gpu"]].append(price)
        
        count += 1

    print(f"[*] Procesados {count} items válidos para PRIME stats.")

    # =========================================================================
    # GENERACIÓN DE ESTADÍSTICAS FINALES
    # =========================================================================
    final_stats = {}
    
    for cat, cond_dict in market_data["PRIME"].items():
        final_stats[cat] = {}
        
        for cond, data in cond_dict.items():
            prices = data["prices"]
            
            # Requerir mínimo 2 muestras para calcular estadísticas
            if len(prices) < 2:
                continue
            
            stats = {
                "mean": round(statistics.mean(prices), 2),
                "median": round(statistics.median(prices), 2),
                "stdev": round(statistics.stdev(prices), 2) if len(prices) > 1 else 0,
                "count": len(prices),
                "components": {}
            }
            
            # Estadísticas por componente
            for ctype, cdata in data["specs"].items():
                stats["components"][ctype] = {}
                for cname, cprices in cdata.items():
                    if len(cprices) >= 2:
                        stats["components"][ctype][cname] = {
                            "mean": round(statistics.mean(cprices), 2),
                            "median": round(statistics.median(cprices), 2),
                            "stdev": round(statistics.stdev(cprices), 2) if len(cprices) > 1 else 0,
                            "count": len(cprices)
                        }
            
            final_stats[cat][cond] = stats
    
    # Añadir estadísticas de segmentos secundarios
    for sec_cat, prices in market_data["SECONDARY"].items():
        if len(prices) > 3:
            final_stats[sec_cat] = {
                "mean": round(statistics.mean(prices), 2),
                "count": len(prices)
            }
    
    # Añadir estadísticas de segmento incierto
    unc_prices = market_data["UNCERTAIN"]["prices"]
    if len(unc_prices) > 3:
        final_stats["UNCERTAIN"] = {
            "mean": round(statistics.mean(unc_prices), 2),
            "count": len(unc_prices)
        }

    # Guardar resultados
    with open(OUTPUT_STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_stats, f, indent=4)
        
    print(f"[OK] Guardado en {OUTPUT_STATS_FILE}")


# =============================================================================
# PUNTO DE ENTRADA PRINCIPAL
# =============================================================================

if __name__ == "__main__":
    # Buscar el archivo de datos más reciente
    # Prioridad: wallapop_raw_full_*.json > wallapop_raw_data_*.json
    list_of_files = glob.glob('wallapop_raw_full_*.json')
    
    if not list_of_files:
        list_of_files = glob.glob('wallapop_raw_data_*.json')
    
    if list_of_files:
        # Seleccionar el archivo más reciente por fecha de creación
        latest_file = max(list_of_files, key=os.path.getctime)
        process_data(latest_file)
    else:
        print("[!] No hay datos raw para procesar.")