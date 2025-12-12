#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Módulo: poller.py
Descripción: Sistema inteligente de recolección y análisis de fraudes en Wallapop.
             Implementa un motor de detección de riesgos basado en estadísticas
             de mercado y análisis heurístico de comportamiento sospechoso.

Funcionalidades principales:
    - Recolección de anuncios de la API de Wallapop en tiempo real
    - Análisis estadístico de precios mediante Z-scores compuestos
    - Detección de patrones de fraude (precios anómalos, cuentas nuevas, etc.)
    - Enriquecimiento con datos de usuarios y reseñas
    - Clasificación por condición del producto (nuevo, usado, etc.)

Dependencias:
    - requests: Cliente HTTP para comunicación con la API
    - regex_analyzer: Módulo de análisis de especificaciones hardware

Uso:
    python poller.py

Notas:
    - Los datos se guardan automáticamente cada 20 minutos
    - Soporta interrupción limpia con Ctrl+C
    - Requiere el archivo market_stats.json para comparación de precios
================================================================================
"""

import json
import time
import random
import requests
import os
import re
from datetime import datetime, timedelta
from typing import Dict, Any

# Importación del módulo de análisis de expresiones regulares
try:
    import regex_analyzer
except ImportError:
    print("[!] Error: No se encuentra 'regex_analyzer.py'.")
    exit(1)


# =============================================================================
# CONSTANTES DE CONFIGURACIÓN
# =============================================================================

# Identificador de la categoría "Informática y Electrónica" en Wallapop
CATEGORY_ID = "24200"

# Subcategoría específica para portátiles
TARGET_SUB_ID = "10310"

# Límite máximo de artículos a procesar por ejecución
MAX_ITEMS_TO_FETCH = 5000

# Intervalo de autoguardado en minutos
SAVE_INTERVAL_MINUTES = 20

# Archivo con estadísticas de mercado para comparación de precios
STATS_FILE = "market_stats.json"

# Pesos para el cálculo del Z-score compuesto
# Determina la importancia relativa de cada componente en la estimación del valor
WEIGHTS = {
    "cpu": 0.5,      # CPU tiene el mayor impacto en el precio
    "gpu": 0.3,      # GPU es relevante especialmente en equipos gaming
    "ram": 0.1,      # RAM tiene impacto moderado
    "category": 0.1  # Precio medio de la categoría como referencia base
}


# =============================================================================
# CARGA DE DATOS DE REFERENCIA
# =============================================================================

def load_market_stats() -> Dict:
    """
    Carga las estadísticas de mercado desde el archivo JSON.
    
    Estas estadísticas contienen precios medios, desviaciones estándar
    y distribuciones por categoría, condición y componentes hardware.
    Son esenciales para calcular los Z-scores y detectar anomalías.
    
    Returns:
        Dict: Diccionario con estadísticas de mercado organizadas por
              categoría y condición, o diccionario vacío si no existe.
    """
    if not os.path.exists(STATS_FILE):
        return {}
    try:
        with open(STATS_FILE, 'r', encoding='utf-8') as f:
            print(f"[*] Estadísticas cargadas.")
            return json.load(f)
    except:
        return {}


# Carga las estadísticas al iniciar el módulo
MARKET_STATS = load_market_stats()


# =============================================================================
# FUNCIONES DE COMUNICACIÓN CON LA API
# =============================================================================

def make_request(url: str, params: dict = None) -> requests.Response:
    """
    Realiza una petición HTTP GET con reintentos y manejo de rate limiting.
    
    Implementa un patrón de backoff exponencial para manejar errores
    transitorios y límites de tasa de la API de Wallapop.
    
    Args:
        url (str): URL del endpoint a consultar.
        params (dict, optional): Parámetros de query string.
    
    Returns:
        requests.Response: Objeto respuesta HTTP, o None si fallan todos
                          los reintentos.
    
    Notas:
        - Máximo 3 reintentos con pausas incrementales
        - En caso de HTTP 429 (rate limit), espera 10 segundos
        - Usa headers que simulan un navegador real para evitar bloqueos
    """
    headers = {
        "Host": "api.wallapop.com",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "X-DeviceOS": "0"
    }
    
    for attempt in range(3):
        try:
            # Pausa incremental entre reintentos con jitter aleatorio
            if attempt > 0:
                time.sleep((attempt * 2) + random.uniform(0, 1))
            
            response = requests.get(url, params=params, headers=headers, timeout=15)
            
            # Manejo de rate limiting
            if response.status_code == 429:
                time.sleep(10)
                continue
                
            return response
            
        except requests.RequestException:
            continue
    
    return None


def get_user_details(user_id: str) -> Dict[str, Any]:
    """
    Obtiene información detallada de un usuario de Wallapop.
    
    Consulta el perfil del vendedor para obtener datos relevantes
    como fecha de registro, badges, tipo de cuenta (pro/particular), etc.
    
    Args:
        user_id (str): Identificador único del usuario en Wallapop.
    
    Returns:
        Dict[str, Any]: Datos del perfil del usuario, o diccionario vacío
                       si la consulta falla.
    """
    url = f"https://api.wallapop.com/api/v3/users/{user_id}"
    
    # Pequeña pausa para evitar saturar la API
    time.sleep(random.uniform(0, 0.001))
    
    response = make_request(url)
    return response.json() if response and response.status_code == 200 else {}


def get_user_reviews_stats(user_id: str) -> Dict[str, float]:
    """
    Calcula estadísticas de reseñas de un vendedor.
    
    Obtiene todas las reseñas del usuario y calcula métricas agregadas
    como el número total de reseñas y la puntuación media.
    
    Args:
        user_id (str): Identificador único del usuario.
    
    Returns:
        Dict[str, float]: Diccionario con:
            - count: Número total de reseñas
            - avg_stars: Puntuación media (0-5 estrellas)
    """
    url = f"https://api.wallapop.com/api/v3/users/{user_id}/reviews"
    
    time.sleep(random.uniform(0, 0.001))
    
    response = make_request(url)
    stats = {"count": 0, "avg_stars": 0.0}
    
    if response and response.status_code == 200:
        try:
            reviews = response.json()
            if isinstance(reviews, list) and reviews:
                count = len(reviews)
                # La puntuación viene en escala 0-100, la convertimos a 0-5
                total = sum(r.get("review", {}).get("scoring", 0) for r in reviews)
                stats["count"] = count
                stats["avg_stars"] = round((total / count / 100) * 5, 2)
        except:
            pass
    
    return stats


def get_item_details_full(item_id: str) -> Dict[str, Any]:
    """
    Obtiene los detalles completos de un artículo.
    
    La búsqueda general solo devuelve información parcial. Esta función
    consulta el endpoint específico del artículo para obtener datos
    adicionales como el estado real (nuevo/usado) desde type_attributes.
    
    Args:
        item_id (str): Identificador único del artículo.
    
    Returns:
        Dict[str, Any]: Datos completos del artículo incluyendo:
            - type_attributes: Condición real del producto
            - counters: Vistas, favoritos, conversaciones
            - shipping: Información de envío
    """
    url = f"https://api.wallapop.com/api/v3/items/{item_id}"
    
    # Pausa mínima para evitar saturación
    time.sleep(random.uniform(0, 0.001))
    
    response = make_request(url)
    return response.json() if response and response.status_code == 200 else {}


# =============================================================================
# FUNCIONES DE TRANSFORMACIÓN Y MAPEO
# =============================================================================

def map_api_condition(api_val: str) -> str:
    """
    Mapea el estado del producto de la API a categorías internas.
    
    Wallapop usa términos específicos en inglés que necesitamos
    normalizar a nuestras categorías estándar para análisis consistente.
    
    Args:
        api_val (str): Valor de condición devuelto por la API.
    
    Returns:
        str: Condición normalizada (NEW, LIKE_NEW, USED, BROKEN).
    
    Mapeo:
        - "new" -> "NEW"
        - "as_good_as_new" -> "LIKE_NEW"
        - "good", "fair" -> "USED"
        - "has_given_it_all" -> "BROKEN"
    """
    if not api_val:
        return None
    
    api_val = api_val.lower()
    
    if api_val == "new":
        return "NEW"
    if api_val == "as_good_as_new":
        return "LIKE_NEW"
    if api_val in ["good", "fair"]:
        return "USED"
    if api_val == "has_given_it_all":
        return "BROKEN"
    
    return "USED"


# =============================================================================
# WRAPPERS DE ANÁLISIS REGEX
# =============================================================================

def get_prioritized_specs_and_category(title: str, description: str):
    """
    Wrapper para extraer especificaciones hardware y categoría.
    
    Delega al módulo regex_analyzer para analizar título y descripción
    y extraer información de CPU, RAM, GPU y clasificar el tipo de equipo.
    
    Args:
        title (str): Título del anuncio.
        description (str): Descripción completa del producto.
    
    Returns:
        Tuple: (specs_dict, category_str, condition_str)
    """
    return regex_analyzer.get_prioritized_specs_and_category(title, description)


def get_stats_for_component(stats_node, component_type, component_name):
    """
    Obtiene estadísticas de mercado para un componente específico.
    
    Busca en el nodo de estadísticas los datos de precio para un
    componente hardware particular (ej: "INTEL I7", "RTX 4070").
    
    Args:
        stats_node: Nodo de estadísticas (ya filtrado por condición).
        component_type (str): Tipo de componente ("cpu", "gpu", "ram").
        component_name (str): Nombre específico del componente.
    
    Returns:
        Dict o None: Estadísticas del componente (mean, stdev, count)
                    o None si no existe.
    """
    if not component_name or not stats_node:
        return None
    try:
        return stats_node["components"][component_type].get(component_name)
    except KeyError:
        return None


# =============================================================================
# MOTOR DE CÁLCULO DE RIESGO
# =============================================================================

def calculate_risk_base(item, force_condition=None):
    """
    Calcula la puntuación de riesgo de fraude para un artículo.
    
    Implementa un sistema de scoring multi-factor que combina:
    - Análisis estadístico de precios (Z-scores)
    - Heurísticas de comportamiento sospechoso
    - Comparación con valores de mercado por componente
    
    Args:
        item (Dict): Diccionario con datos del artículo (título, descripción,
                    precio, usuario, etc.)
        force_condition (str, optional): Condición verificada desde la API.
                                        Tiene prioridad sobre la detectada por regex.
    
    Returns:
        Dict: Resultado del análisis con estructura:
            - risk_score (int): Puntuación 0-100 (mayor = más riesgo)
            - risk_factors (List[str]): Lista de factores de riesgo detectados
            - market_analysis (Dict): Detalles del análisis de mercado
    
    Algoritmo de puntuación:
        - Precio estadísticamente bajo (Z < -1.5): +30 puntos
        - Anomalía extrema (Z < -2.5): +40 puntos adicionales
        - Precio < 40% del valor estimado: +20 puntos
        - Descripción corta (< 30 chars) con precio alto: +15 puntos
        - Contacto externo (WhatsApp, teléfono): +30 puntos
    """
    score = 0
    factors = []
    
    title = item.get("title", "")
    desc = item.get("description", "")
    price = regex_analyzer.clean_price(item)
    
    # Extraer especificaciones hardware y categoría del texto
    specs, category, regex_condition = get_prioritized_specs_and_category(title, description=desc)
    
    # La condición de la API tiene prioridad sobre la detectada por regex
    condition = force_condition if force_condition else regex_condition
    
    # Buscar estadísticas de mercado por categoría y condición
    # Estructura: CATEGORY -> CONDITION -> SPECS
    cat_root = MARKET_STATS.get(category, {})
    stats_node = cat_root.get(condition, {})
    
    # Fallback: si no hay datos para el estado exacto, usamos estados similares
    fallback_used = False
    if not stats_node:
        if condition == "NEW":
            # Productos nuevos sin stats: comparamos con casi nuevos o usados
            stats_node = cat_root.get("LIKE_NEW") or cat_root.get("USED")
            fallback_used = True
        elif condition == "LIKE_NEW":
            stats_node = cat_root.get("USED")
            fallback_used = True
    
    if not stats_node:
        stats_node = {}

    # -------------------------------------------------------------------------
    # CASO ESPECIAL: Precio simbólico (< 5€)
    # Estos artículos no entran en el análisis estadístico normal
    # -------------------------------------------------------------------------
    if price < 5.0:
        return {
            "risk_score": 0,
            "risk_factors": ["Symbolic Price"],
            "market_analysis": {
                "detected_category": "UNCERTAIN_PRICE",
                "detected_condition": condition,
                "specs_detected": specs,
                "composite_z_score": 0,
                "estimated_market_value": 0,
                "components_used": []
            }
        }

    # -------------------------------------------------------------------------
    # CÁLCULO DEL Z-SCORE COMPUESTO
    # Combina múltiples señales de precio con pesos configurables
    # -------------------------------------------------------------------------
    signals = []
    
    # Z-score por cada componente hardware detectado
    for comp in ["cpu", "gpu", "ram"]:
        c_stats = get_stats_for_component(stats_node, comp, specs.get(comp))
        if c_stats and c_stats["stdev"] > 0:
            z = (price - c_stats["mean"]) / c_stats["stdev"]
            signals.append({
                "z": z,
                "weight": WEIGHTS[comp],
                "ref": c_stats["mean"],
                "src": f"{comp}:{specs.get(comp)}"
            })

    # Z-score basado en la media de la categoría
    if stats_node.get("stdev", 0) > 0:
        z_cat = (price - stats_node["mean"]) / stats_node["stdev"]
        signals.append({
            "z": z_cat,
            "weight": WEIGHTS["category"],
            "ref": stats_node["mean"],
            "src": category
        })

    # Calcular Z-score final ponderado y valor estimado
    final_z = 0
    est_val = 0
    
    if signals:
        w_z_sum = sum(s["z"] * s["weight"] for s in signals)
        w_p_sum = sum(s["ref"] * s["weight"] for s in signals)
        tot_w = sum(s["weight"] for s in signals)
        
        if tot_w > 0:
            final_z = w_z_sum / tot_w
            est_val = w_p_sum / tot_w
            
            # Ajuste: productos nuevos comparados con usados valen más
            if fallback_used and condition == "NEW":
                est_val *= 1.2  # +20% para productos nuevos
                stdev_ref = stats_node.get("stdev", 100)
                final_z = (price - est_val) / stdev_ref

    # -------------------------------------------------------------------------
    # FACTORES DE RIESGO BASADOS EN PRECIO
    # -------------------------------------------------------------------------
    if final_z < -1.5:
        score += 30
        factors.append(f"Statistically Cheap (Z={final_z:.2f}) [{condition}]")
    
    if final_z < -2.5:
        score += 40
        factors.append("EXTREME Price Anomaly")

    # -------------------------------------------------------------------------
    # HEURÍSTICAS ADICIONALES DE DETECCIÓN
    # -------------------------------------------------------------------------
    
    # Descripción muy corta para un producto caro = sospechoso
    if len(desc) < 30 and price > 200:
        score += 15
        factors.append("Short Desc")
    
    # Patrón de contacto externo (intento de evadir la plataforma)
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


# =============================================================================
# BUCLE PRINCIPAL DE RECOLECCIÓN
# =============================================================================

def run_smart_poller():
    """
    Ejecuta el proceso principal de recolección y análisis de anuncios.
    
    Este es el punto de entrada del sistema de detección de fraudes.
    Realiza las siguientes operaciones en bucle:
    
    1. Consulta la API de Wallapop para obtener anuncios recientes
    2. Filtra por fecha (últimas 24 horas)
    3. Corrige precios ocultos en descripciones
    4. Obtiene detalles completos de cada artículo
    5. Calcula puntuación de riesgo
    6. Enriquece con datos de usuario si hay indicios de fraude
    7. Guarda resultados periódicamente
    
    La función maneja interrupciones (Ctrl+C) de forma limpia,
    guardando los datos recolectados antes de terminar.
    
    Returns:
        None
    
    Archivos generados:
        wallapop_smart_data_YYYYMMDD.json - Datos en formato NDJSON
    """
    url = "https://api.wallapop.com/api/v3/search"
    
    # Parámetros de búsqueda para la API de Wallapop
    params = {
        "category_id": CATEGORY_ID,
        "subcategory_ids": TARGET_SUB_ID,
        "source": "side_bar_filters",
        "order_by": "newest",      # Ordenar por más recientes
        "time_filter": "today",    # Solo anuncios de hoy
        "latitude": "40.4168",     # Coordenadas de Madrid (centro España)
        "longitude": "-3.7038",
    }
    
    # Ventana temporal: solo procesamos anuncios de las últimas 24 horas
    cutoff_date = datetime.now() - timedelta(hours=24)
    
    print("--- SMART POLLER v3 (Always Full Details & Condition-Aware Pricing) ---")
    print(f"[*] Fecha de corte (24h): {cutoff_date.isoformat()}")
    
    all_items = []
    next_page_token = None
    last_save = datetime.now()
    
    try:
        # =====================================================================
        # BUCLE DE PAGINACIÓN
        # Continúa hasta alcanzar el límite o agotar los resultados
        # =====================================================================
        while len(all_items) < MAX_ITEMS_TO_FETCH:
            # Añadir token de paginación si no es la primera página
            if next_page_token:
                params["next_page"] = next_page_token
            
            response = make_request(url, params)
            if not response:
                break
            
            data = response.json()
            
            # Extraer items de la respuesta (formato puede variar)
            items = []
            if "data" in data and "section" in data["data"]:
                items = data["data"]["section"]["payload"].get("items", [])
            elif "items" in data:
                items = data.get("items", [])
            
            if not items:
                break
            
            print(f"    [+] Analizando lote de {len(items)} items...")
            
            # =================================================================
            # PROCESAMIENTO DE CADA ARTÍCULO
            # =================================================================
            for item in items:
                # -------------------------------------------------------------
                # FILTRO DE FECHA
                # Saltamos artículos más antiguos de 24 horas
                # -------------------------------------------------------------
                ts = item.get("modified_date") or item.get("creation_date")
                if ts:
                    try:
                        item_dt = datetime.fromtimestamp(ts / 1000)
                        if item_dt < cutoff_date:
                            continue
                    except:
                        pass

                # -------------------------------------------------------------
                # CORRECCIÓN DE PRECIOS OCULTOS
                # Algunos vendedores ponen precio simbólico y el real en texto
                # -------------------------------------------------------------
                original_price = regex_analyzer.clean_price(item)
                corrected_price = False
                
                if original_price < 5.0:
                    real = regex_analyzer.try_extract_hidden_price(
                        item.get("title", ""),
                        item.get("description", "")
                    )
                    if real:
                        item["price"] = {"amount": real, "currency": "EUR"}
                        corrected_price = True
                
                # Ignorar artículos sin precio válido
                if regex_analyzer.clean_price(item) < 1.0 and not corrected_price:
                    continue

                # -------------------------------------------------------------
                # OBTENCIÓN DE DETALLES COMPLETOS
                # Necesario para conocer la condición real del producto
                # -------------------------------------------------------------
                item_id = item.get("id")
                real_condition = None
                
                if item_id:
                    details = get_item_details_full(item_id)
                    
                    # Extraer condición desde type_attributes
                    api_cond_val = None
                    type_attrs = details.get("type_attributes", {})
                    if type_attrs and "condition" in type_attrs:
                        api_cond_val = type_attrs["condition"].get("value")
                    
                    real_condition = map_api_condition(api_cond_val)
                    
                    # Flag de producto reacondicionado
                    if details.get("is_refurbished", {}).get("flag") is True:
                        real_condition = "LIKE_NEW"

                    # Guardar métricas adicionales para análisis en Kibana
                    item["counters"] = details.get("counters")
                    item["shipping_allowed"] = details.get("shipping", {}).get("user_allows_shipping")

                # -------------------------------------------------------------
                # CÁLCULO DE RIESGO
                # Usando la condición verificada de la API
                # -------------------------------------------------------------
                risk_data = calculate_risk_base(item, force_condition=real_condition)
                
                if real_condition:
                    risk_data["risk_factors"].append(f"Verified Condition: {real_condition}")

                # -------------------------------------------------------------
                # ENRIQUECIMIENTO CON DATOS DE USUARIO
                # Solo para artículos con indicios de riesgo (optimización)
                # -------------------------------------------------------------
                should_enrich_user = False
                
                if risk_data["market_analysis"].get("composite_z_score", 0) < -1.5:
                    should_enrich_user = True
                if "External Contact" in str(risk_data["risk_factors"]):
                    should_enrich_user = True
                if corrected_price:
                    should_enrich_user = True

                if should_enrich_user:
                    user_id = item.get("user", {}).get("id") or item.get("user_id")

                    if user_id:
                        u_details = get_user_details(user_id)
                        u_stats = get_user_reviews_stats(user_id)
                        
                        # Ajustes por reputación del vendedor
                        sales = u_stats["count"]
                        stars = u_stats["avg_stars"]
                        badges = u_details.get("badges", [])
                        is_top = "TOP" in str(badges).upper() or u_details.get("type") == "pro"
                        
                        # Vendedor con buena reputación = menos riesgo
                        if sales > 5 and stars >= 4.5:
                            risk_data["risk_score"] -= 30
                            risk_data["risk_factors"].append(f"Trusted Seller ({sales}+ reviews)")
                        
                        if is_top:
                            risk_data["risk_score"] -= 50
                            risk_data["risk_factors"].append("TOP SELLER")

                        # Análisis de antigüedad de cuenta
                        reg_ts = u_details.get("register_date")
                        if reg_ts:
                            days = (datetime.now() - datetime.fromtimestamp(reg_ts/1000)).days
                            
                            # Cuenta muy nueva = mayor riesgo
                            if days < 3:
                                risk_data["risk_score"] += 30
                                risk_data["risk_factors"].append("New User")
                            
                            # Cuenta antigua sin ventas = posible cuenta robada/dormant
                            if days > 365 and sales == 0:
                                risk_data["risk_score"] += 20
                                risk_data["risk_factors"].append("Dormant Account")
                        
                        # Cuenta con reportes de estafa = riesgo máximo
                        if u_details.get("scam_reports", 0) > 0:
                            risk_data["risk_score"] = 100
                            risk_data["risk_factors"].append("REPORTED SCAMMER")

                # Normalizar puntuación al rango 0-100
                risk_data["risk_score"] = max(0, min(100, risk_data["risk_score"]))
                item["enrichment"] = risk_data
                
                # -------------------------------------------------------------
                # PREPARACIÓN DE GEOLOCALIZACIÓN PARA ELASTICSEARCH
                # Formato geo_point para mapas en Kibana
                # -------------------------------------------------------------
                loc = item.get("location", {})
                if "latitude" in loc and "longitude" in loc:
                    loc["geo"] = {"lat": loc["latitude"], "lon": loc["longitude"]}

                # Añadir timestamp de recolección
                item["timestamps"] = {"crawl_timestamp": datetime.now().isoformat()}
                
                all_items.append(item)
                
                # Mostrar alertas para artículos de alto riesgo
                if risk_data["risk_score"] >= 50:
                    print(f"        [!] ALERTA (Score {risk_data['risk_score']}): {item.get('title')[:40]}...")

            # -----------------------------------------------------------------
            # AUTOGUARDADO PERIÓDICO
            # Protege contra pérdida de datos por interrupciones
            # -----------------------------------------------------------------
            if (datetime.now() - last_save).total_seconds() > (SAVE_INTERVAL_MINUTES * 60):
                fname = f"wallapop_smart_data_{datetime.now().strftime('%Y%m%d')}.json"
                print(f"\n    [DISK] Autoguardando progreso ({len(all_items)} items) en {fname}...")
                with open(fname, "w", encoding="utf-8") as f:
                    for i in all_items:
                        f.write(json.dumps(i, ensure_ascii=False) + "\n")
                last_save = datetime.now()

            # Obtener token para la siguiente página
            next_page_token = data.get("meta", {}).get("next_page")
            if not next_page_token:
                break
    
    except KeyboardInterrupt:
        # Manejo limpio de interrupción por usuario
        print("\n\n[!] Detenido por el usuario (Ctrl+C). Guardando datos recolectados...")

    # =========================================================================
    # GUARDADO FINAL
    # =========================================================================
    if all_items:
        fname = f"wallapop_smart_data_{datetime.now().strftime('%Y%m%d')}.json"
        with open(fname, "w", encoding="utf-8") as f:
            for i in all_items:
                f.write(json.dumps(i, ensure_ascii=False) + "\n")
        print(f"[OK] {len(all_items)} items guardados en {fname}")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    run_smart_poller()