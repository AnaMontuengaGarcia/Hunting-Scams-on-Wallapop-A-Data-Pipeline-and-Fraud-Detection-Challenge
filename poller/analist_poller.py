#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Módulo: analist_poller.py
Descripción: Recolector profundo de datos de Wallapop para análisis estadístico.
             Este módulo realiza una recolección masiva de anuncios con todos
             sus detalles para generar estadísticas de mercado precisas.

Diferencias con poller.py:
    - Este módulo está optimizado para recolección masiva (hasta 50,000 items)
    - Obtiene detalles completos de TODOS los artículos (deep fetch)
    - No realiza cálculo de riesgo en tiempo real
    - Genera datos raw para alimentar regex_analyzer.py

Funcionalidades principales:
    - Recolección masiva con paginación automática
    - Obtención de atributos detallados (condición, métricas, envío)
    - Guardado con checkpoints para recuperación ante fallos
    - Manejo de interrupciones con señales POSIX

Dependencias:
    - requests: Cliente HTTP para comunicación con la API
    - signal: Manejo de señales del sistema operativo

Uso:
    python analist_poller.py

Notas:
    - Los datos se guardan en formato JSON (no NDJSON)
    - Incluye mecanismo de checkpoint cada 10 minutos
    - Soporta interrupción limpia con Ctrl+C
================================================================================
"""

import json
import time
import random
import requests
import signal
import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any


# =============================================================================
# CONSTANTES DE CONFIGURACIÓN
# =============================================================================

# Identificador de la categoría "Informática y Electrónica" en Wallapop
CATEGORY_ID = "24200"

# Subcategoría específica para portátiles
TARGET_SUB_ID = "10310"

# Límite máximo de artículos a recolectar
# Nota: Reducido a 50k debido a la lentitud del deep fetch
MAX_ITEMS_LIMIT = 50000

# Días hacia atrás para recolectar datos históricos
DAYS_TO_RETRIEVE = 30

# Intervalo de guardado automático en minutos
SAVE_INTERVAL_MINUTES = 10

# Número máximo de reintentos para peticiones HTTP
MAX_RETRIES = 3


# =============================================================================
# MANEJO DE SEÑALES E INTERRUPCIONES
# =============================================================================

# Flag global para control de interrupción limpia
interrupted = False


def signal_handler(sig, frame):
    """
    Manejador de señales del sistema operativo.
    
    Captura la señal SIGINT (Ctrl+C) y activa el flag de interrupción
    para permitir un cierre limpio del proceso con guardado de datos.
    
    Args:
        sig: Número de señal recibida.
        frame: Frame de pila actual (no utilizado).
    
    Returns:
        None
    """
    global interrupted
    print("\n\n[!!!] Interrupción recibida (Ctrl+C). Deteniendo limpiamente...")
    interrupted = True


# Registrar el manejador para SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, signal_handler)


# =============================================================================
# FUNCIONES DE COMUNICACIÓN CON LA API
# =============================================================================

def make_request(url: str, params: dict = None) -> requests.Response:
    """
    Realiza una petición HTTP GET con reintentos y backoff exponencial.
    
    Implementa manejo robusto de errores incluyendo:
    - Rate limiting (HTTP 403, 429)
    - Errores de servidor (HTTP 5xx)
    - Errores de red y timeouts
    
    Args:
        url (str): URL del endpoint a consultar.
        params (dict, optional): Parámetros de query string.
    
    Returns:
        requests.Response: Objeto respuesta HTTP, o None si:
            - Se alcanza el límite de reintentos
            - Se recibe una señal de interrupción
    
    Notas:
        - Backoff exponencial: 1.5^attempt + ruido aleatorio
        - En caso de rate limit, pausa de 60 segundos
        - Headers simulan un navegador real para evitar bloqueos
    """
    headers = {
        "Host": "api.wallapop.com",
        "Accept": "application/json, text/plain, */*",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
        "X-DeviceOS": "0",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    for attempt in range(MAX_RETRIES):
        # Verificar si se ha solicitado interrupción
        if interrupted:
            return None
        
        try:
            # Backoff exponencial con componente aleatorio (jitter)
            wait = (1.5 ** attempt) + random.uniform(0.1, 0.5)
            if attempt > 0:
                time.sleep(wait)
            
            response = requests.get(url, params=params, headers=headers, timeout=15)
            
            # Manejo de rate limiting y bloqueos temporales
            if response.status_code == 403 or response.status_code == 429:
                print(f"    [PAUSA] Rate Limit/Block ({response.status_code}). Esperando 60s...")
                time.sleep(60)
                continue
            
            # Reintentar en errores de servidor
            if response.status_code >= 500:
                continue
                
            return response
            
        except requests.RequestException as e:
            print(f"    [NET] Error red: {e}")
            continue
            
    return None


def get_item_details_full(item_id: str) -> Dict[str, Any]:
    """
    Obtiene los detalles completos de un artículo desde la API.
    
    Consulta el endpoint específico del artículo para obtener
    información detallada que no está disponible en los resultados
    de búsqueda, como:
    - Estado real del producto (type_attributes.condition)
    - Métricas de demanda (vistas, favoritos, conversaciones)
    - Información de envío y reserva
    
    Args:
        item_id (str): Identificador único del artículo en Wallapop.
    
    Returns:
        Dict[str, Any]: Datos completos del artículo, o diccionario
                       vacío si la consulta falla.
    
    Notas:
        - Incluye pausa obligatoria para evitar bans por rate limiting
        - Esta función se llama para CADA artículo (deep fetch)
    """
    url = f"https://api.wallapop.com/api/v3/items/{item_id}"
    
    # Pausa mínima entre peticiones de detalles
    time.sleep(random.uniform(0, 0.01))
    
    resp = make_request(url)
    if resp and resp.status_code == 200:
        return resp.json()
    return {}


# =============================================================================
# FUNCIONES DE PERSISTENCIA
# =============================================================================

def save_checkpoint(items: List[Dict[str, Any]], filename: str) -> None:
    """
    Guarda un checkpoint de los datos recolectados.
    
    Implementa escritura atómica mediante archivo temporal para
    evitar corrupción de datos en caso de fallo durante la escritura.
    
    Args:
        items (List[Dict[str, Any]]): Lista de artículos a guardar.
        filename (str): Nombre del archivo destino.
    
    Returns:
        None
    
    Proceso:
        1. Escribir a archivo temporal (.tmp)
        2. Reemplazar/renombrar al nombre final
        3. Esto garantiza que el archivo final siempre esté completo
    
    Notas:
        - Los datos se guardan en formato JSON con indentación
        - A diferencia del poller.py, aquí NO se usa NDJSON
    """
    temp_filename = filename + ".tmp"
    try:
        with open(temp_filename, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=4)
        
        # Reemplazo atómico del archivo
        if os.path.exists(filename):
            os.replace(temp_filename, filename)
        else:
            os.rename(temp_filename, filename)
        
        print(f"    [DISK] Checkpoint guardado: {len(items)} ítems.")
        
    except Exception as e:
        print(f"    [!] Error guardando checkpoint: {e}")


# =============================================================================
# FUNCIÓN PRINCIPAL DE RECOLECCIÓN
# =============================================================================

def run_collector() -> None:
    """
    Ejecuta el proceso de recolección profunda de datos.
    
    Este es el punto de entrada principal del recolector. Realiza
    una búsqueda paginada en la API de Wallapop y obtiene detalles
    completos de cada artículo encontrado.
    
    Flujo de ejecución:
        1. Configurar parámetros de búsqueda
        2. Iterar sobre páginas de resultados
        3. Para cada artículo, obtener detalles completos (deep fetch)
        4. Enriquecer con campos adicionales de la API
        5. Guardar checkpoints periódicamente
        6. Finalizar con guardado completo
    
    Datos recolectados por artículo:
        - type_attributes: Condición real del producto
        - is_refurbished: Flag de reacondicionado
        - taxonomy: Clasificación interna de Wallapop
        - counters: Vistas, favoritos, conversaciones
        - shipping: Opciones de envío
        - reserved: Estado de reserva
        - description: Descripción completa (sin truncar)
    
    Returns:
        None
    
    Archivos generados:
        wallapop_raw_full_YYYYMMDD_HHMM.json
    """
    url = "https://api.wallapop.com/api/v3/search"
    
    # Parámetros de búsqueda para la API
    params = {
        "category_id": CATEGORY_ID,
        "subcategory_ids": TARGET_SUB_ID,
        "source": "side_bar_filters",
        "country_code": "ES",
        "order_by": "newest",      # Ordenar por más recientes primero
        "latitude": "40.4168",     # Centro de España (Madrid)
        "longitude": "-3.7038",
    }
    
    # Inicialización de variables de control
    all_items = []
    next_page = None
    cutoff_date = datetime.now() - timedelta(days=DAYS_TO_RETRIEVE)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M')
    filename = f"wallapop_raw_full_{timestamp}.json"
    
    print(f"[*] Iniciando Recolección Profunda (Deep Fetch). Meta: {MAX_ITEMS_LIMIT} items.")
    print(f"    Archivo: {filename}")
    
    last_save = datetime.now()
    stop_fetching = False
    
    # =========================================================================
    # BUCLE PRINCIPAL DE RECOLECCIÓN
    # =========================================================================
    while len(all_items) < MAX_ITEMS_LIMIT and not stop_fetching and not interrupted:
        # Añadir token de paginación si existe
        if next_page:
            params["next_page"] = next_page
        
        try:
            # Pausa entre páginas de búsqueda para evitar rate limiting
            time.sleep(random.uniform(0, 0.5))
            
            resp = make_request(url, params)
            if not resp or resp.status_code != 200:
                break
                
            data = resp.json()
            
            # Extraer items del response (el formato puede variar)
            items_batch = []
            if "data" in data and "section" in data["data"]:
                items_batch = data["data"]["section"]["payload"].get("items", [])
            elif "items" in data:
                items_batch = data.get("items", [])
            
            if not items_batch:
                break
                
            print(f"    [PÁGINA] Procesando lote de {len(items_batch)} items...")
            
            added_in_batch = 0
            
            # -----------------------------------------------------------------
            # PROCESAMIENTO DE CADA ARTÍCULO DEL LOTE
            # -----------------------------------------------------------------
            for item in items_batch:
                # Verificar interrupción
                if interrupted:
                    break
                
                # Filtro por fecha de creación
                ts = item.get("creation_date")
                if ts:
                    if datetime.fromtimestamp(ts / 1000) < cutoff_date:
                        stop_fetching = True
                        break
                
                # =============================================================
                # DEEP FETCH: Obtención de detalles completos
                # Esta es la diferencia principal con el poller normal
                # =============================================================
                item_id = item.get("id")
                if item_id:
                    details = get_item_details_full(item_id)
                    
                    if details:
                        # 1. ESTADO DEL PRODUCTO (Crítico para estadísticas)
                        item["type_attributes"] = details.get("type_attributes")
                        item["is_refurbished"] = details.get("is_refurbished")
                        item["taxonomy"] = details.get("taxonomy")
                        
                        # 2. MÉTRICAS DE DEMANDA (Crítico para detección de fraude)
                        # Incluye: views, favorites, conversations
                        item["counters"] = details.get("counters")
                        
                        # 3. DATOS DE ENVÍO Y RESERVA
                        item["shipping"] = details.get("shipping")
                        item["reserved"] = details.get("reserved")
                        
                        # 4. DESCRIPCIÓN COMPLETA
                        # La búsqueda a veces trunca la descripción
                        full_desc = details.get("description", {}).get("original")
                        if full_desc and len(full_desc) > len(item.get("description", "")):
                            item["description"] = full_desc

                all_items.append(item)
                added_in_batch += 1
                
                # Feedback visual de progreso
                if len(all_items) % 10 == 0:
                    sys.stdout.write(f"\r    -> Total: {len(all_items)}/{MAX_ITEMS_LIMIT}")
                    sys.stdout.flush()
            
            print("")  # Nueva línea tras el lote
            
            # -----------------------------------------------------------------
            # CHECKPOINT PERIÓDICO
            # -----------------------------------------------------------------
            if (datetime.now() - last_save).total_seconds() > (SAVE_INTERVAL_MINUTES * 60):
                save_checkpoint(all_items, filename)
                last_save = datetime.now()
            
            # Obtener token para siguiente página
            if not stop_fetching:
                next_page = data.get("meta", {}).get("next_page")
                if not next_page:
                    break
                    
        except Exception as e:
            print(f"[!] Error: {e}")
            break
    
    # =========================================================================
    # GUARDADO FINAL
    # =========================================================================
    print(f"\n[*] Finalizando. Guardando {len(all_items)} ítems...")
    save_checkpoint(all_items, filename)
    print("[OK] Recolección completada.")


# =============================================================================
# PUNTO DE ENTRADA
# =============================================================================

if __name__ == "__main__":
    run_collector()