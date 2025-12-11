# 🔍 Detección de Fraudes en Wallapop

Pipeline de monitorización y detección de anomalías para anuncios de portátiles en Wallapop, implementando análisis estadístico avanzado y alertas en tiempo real.

---

## Índice

- [Descripción](#descripción)
- [Arquitectura](#arquitectura)
- [Características principales](#características-principales)
- [Requisitos previos](#requisitos-previos)
- [Instalación](#instalación)
- [Configuración](#configuración)
- [Uso](#uso)
- [Estructura del proyecto](#estructura-del-proyecto)
- [Componentes del sistema](#componentes-del-sistema)
- [Modelo de detección de riesgo](#modelo-de-detección-de-riesgo)
- [Visualización con Kibana](#visualización-con-kibana)
- [Licencia](#licencia)

---

## Descripción

Este proyecto implementa un sistema completo de **detección de fraudes** en la plataforma Wallapop, centrado específicamente en la categoría de **portátiles** (subcategoría 10310). El sistema recolecta anuncios de forma diaria, analiza sus características mediante técnicas estadísticas y heurísticas, y genera alertas automáticas cuando detecta patrones sospechosos.

### Problema que resuelve

Los marketplaces de segunda mano como Wallapop son objetivo frecuente de estafadores que publican anuncios con:
- Precios anormalmente bajos para atraer víctimas
- Cuentas recién creadas sin historial verificable
- Descripciones genéricas o copiadas
- Solicitud de contacto por canales externos (WhatsApp, email)

Este pipeline automatiza la detección de estos patrones, permitiendo identificar anuncios de alto riesgo antes de que los usuarios caigan en posibles estafas.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ARQUITECTURA DEL SISTEMA                          │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐         ┌──────────────┐         ┌──────────────┐
    │   Wallapop   │  HTTP   │    Poller    │  JSON   │  Bulk Ingest │
    │     API      │────────▶│   (Python)   │────────▶│   (Python)   │
    └──────────────┘         └──────────────┘         └──────────────┘
                                    │                        │
                                    │ Análisis               │ Indexación
                                    ▼                        ▼
                            ┌──────────────┐         ┌──────────────┐
                            │    Regex     │         │Elasticsearch │
                            │   Analyzer   │         │    Índice    │
                            └──────────────┘         └──────────────┘
                                                            │
                                    ┌───────────────────────┼───────────────────────┐
                                    │                       │                       │
                                    ▼                       ▼                       ▼
                            ┌──────────────┐         ┌──────────────┐       ┌──────────────┐
                            │  ElastAlert  │         │    Kibana    │       │     ILM      │
                            │   (Alertas)  │         │ (Dashboard)  │       │  (Rotación)  │
                            └──────────────┘         └──────────────┘       └──────────────┘
                                    │
                                    ▼
                            ┌──────────────┐
                            │    Email     │
                            │   (SMTP)     │
                            └──────────────┘
```

---

## Características principales

### Recolección de datos
- Conexión directa con la API de Wallapop
- Paginación automática con manejo de rate limiting
- Enriquecimiento con datos de perfil del vendedor y reseñas
- Guardado automático con checkpoints para recuperación ante fallos

### Análisis inteligente
- **Extracción de especificaciones hardware** mediante expresiones regulares (CPU, RAM, GPU)
- **Clasificación automática** en categorías: Apple, Gaming, Workstation, Chromebook, etc.
- **Detección de condición** del producto (Nuevo, Como Nuevo, Usado, Para Piezas)
- **Cálculo de Z-scores compuestos** para identificar anomalías de precio

### Motor de riesgo
- Puntuación de riesgo de 0 a 100 basada en múltiples factores
- Comparación con estadísticas de mercado por categoría y condición
- Detección de patrones de fraude (cuentas nuevas, contacto externo, etc.)

### Alertas en tiempo real
- Integración con ElastAlert2 para monitorización continua
- Notificaciones por email cuando se detectan artículos de alto riesgo
- Reglas configurables con umbrales personalizables

### Almacenamiento y visualización
- Indexación en Elasticsearch con mappings optimizados
- Políticas de retención automática (ILM) con rotación por tamaño/edad
- Dashboards de Kibana para exploración visual de datos

---

## Requisitos previos

### Software necesario
- **Python** 3.10 o superior
- **Elasticsearch** 7.x o 8.x
- **Kibana** 7.x o 8.x (opcional, para visualización)
- **ElastAlert2** 2.16+ (opcional, para alertas)

### Dependencias Python
```
requests>=2.31.0
elasticsearch>=7.9.10
elastalert2>=2.16.0
```

---

## Instalación

### 1. Clonar el repositorio

```bash
git clone https://github.com/AnaMontuengaGarcia/Hunting-Scams-on-Wallapop-A-Data-Pipeline-and-Fraud-Detection-Challenge.git
cd Hunting-Scams-on-Wallapop-A-Data-Pipeline-and-Fraud-Detection-Challenge
```

### 2. Crear y activar entorno virtual

```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 4. Configurar Elasticsearch

Crear la política ILM para rotación automática de índices:

```bash
# Ejecutar en Kibana Dev Tools o mediante la API
PUT _ilm/policy/lab10310-wallapop-rotation
{
  "policy": {
    "phases": {
      "hot": {
        "actions": {
          "rollover": {
            "max_size": "1gb",
            "max_age": "1d"
          }
        }
      },
      "delete": {
        "min_age": "30d",
        "actions": {
          "delete": {}
        }
      }
    }
  }
}
```

Crear el template de índice:

```bash
PUT _index_template/lab10310-wallapop-template
{
  "index_patterns": ["lab10310.wallapop*"],
  "template": {
    "settings": {
      "index.lifecycle.name": "lab10310-wallapop-rotation",
      "index.lifecycle.rollover_alias": "lab10310.wallapop"
    }
  }
}
```

Crear el índice inicial con alias de escritura:

```bash
PUT lab10310.wallapop-000001
{
  "aliases": {
    "lab10310.wallapop": {
      "is_write_index": true
    }
  }
}
```

---

## Configuración

### Rutas del proyecto

Editar el script `run_pipeline.sh` para ajustar las rutas a tu entorno:

```bash
# Directorio base del proyecto
BASE_DIR="/ruta/a/tu/proyecto"

# Ruta al entorno virtual
VENV_DIR="$BASE_DIR/venv"
```

### Configuración de alertas (ElastAlert)

Editar `elastalert/config.yaml`:

```yaml
es_host: localhost
es_port: 9200
# Descomentar si Elasticsearch requiere autenticación:
# es_username: elastic
# es_password: tu_password
```

Editar `elastalert/rules/high_risk.yaml` para configurar los destinatarios de email:

```yaml
email:
- "tu_email@ejemplo.com"

smtp_host: "smtp.gmail.com"
smtp_port: 587
smtp_auth_file: "/ruta/a/smtp_auth.yaml"
```

Crear el archivo de credenciales SMTP (`smtp_auth.yaml`):

```yaml
user: "tu_email@gmail.com"
password: "tu_contraseña_de_aplicacion"
```

---

## Uso

### Ejecución manual del pipeline

```bash
# Activar entorno virtual
source venv/bin/activate

# Ejecutar el poller (recolección de datos)
python3 poller/poller.py

# Ingestar datos a Elasticsearch
python3 ingestion/bulk_ingest.py wallapop_smart_data_YYYYMMDD.json
```

### Ejecución automatizada

Ejecutar el script orquestador que encadena poller + ingesta:

```bash
./run_pipeline.sh
```

### Programación con cron

Para ejecutar el pipeline periódicamente (por ejemplo, cada 6 horas):

```bash
# Editar crontab
crontab -e

# Añadir línea:
0 */6 * * * /ruta/al/proyecto/run_pipeline.sh >> /var/log/wallapop.log 2>&1
```

### Generar estadísticas de mercado

Para actualizar las estadísticas de referencia utilizadas en el cálculo de Z-scores:

```bash
# Recolección masiva de datos
python3 poller/analist_poller.py

# Generar estadísticas
python3 poller/regex_analyzer.py
```

---

## Estructura del proyecto

```
├── README.md                    # Este archivo
├── requirements.txt             # Dependencias Python
├── run_pipeline.sh              # Script orquestador del pipeline
├── market_stats.json            # Estadísticas de mercado de referencia
│
├── poller/                      # Módulos de recolección
│   ├── poller.py                # Poller principal con análisis de riesgo
│   ├── analist_poller.py        # Poller masivo para generación de estadísticas
│   ├── regex_analyzer.py        # Motor de extracción de especificaciones
│   └── config.json              # Configuración del poller
│
├── ingestion/                   # Módulos de carga a Elasticsearch
│   ├── bulk_ingest.py           # Script de ingesta masiva (Bulk API)
│   ├── index_template.json      # Template de índice con mappings
│   ├── index_alias.json         # Configuración del alias de escritura
│   └── ilm_policy.json          # Política de ciclo de vida del índice
│
├── elastalert/                  # Configuración de alertas
│   ├── config.yaml              # Configuración global de ElastAlert
│   └── rules/
│       └── high_risk.yaml       # Regla de alerta para items de alto riesgo
│
└── kibana/                      # Dashboards y visualizaciones
    └── dashboard_export.ndjson  # Exportación del dashboard principal
```

---

## Componentes del sistema

### `poller/poller.py`

Módulo principal de recolección que:
- Consulta la API de Wallapop para la subcategoría de portátiles
- Enriquece cada artículo con información del vendedor y reseñas
- Calcula la puntuación de riesgo en tiempo real
- Guarda los datos en formato NDJSON para ingesta posterior

### `poller/regex_analyzer.py`

Motor de análisis de texto que extrae:
- **CPU**: Intel Core i3/i5/i7/i9, AMD Ryzen, Apple M1/M2/M3, etc.
- **RAM**: Detecta cantidades en GB evitando confusión con almacenamiento
- **GPU**: NVIDIA GeForce RTX/GTX, AMD Radeon RX
- **Categoría**: Apple, Gaming, Workstation, Chromebook, Ultrabook, etc.
- **Condición**: Nuevo, Como Nuevo, Usado, Para Piezas/Roto

### `ingestion/bulk_ingest.py`

Script de carga eficiente que:
- Lee archivos NDJSON en streaming (sin cargar en memoria)
- Agrupa documentos en lotes de 1000 para optimizar rendimiento
- Utiliza la Bulk API de Elasticsearch
- Maneja errores parciales sin interrumpir el proceso

### `elastalert/rules/high_risk.yaml`

Regla de alertas que dispara notificaciones cuando:
- Se indexa un artículo con `risk_score >= 80`
- Incluye en el email: motivos de riesgo, título y enlace al anuncio

---

## Modelo de detección de riesgo

El sistema calcula una puntuación de riesgo (0-100) basada en múltiples factores:

### Factores estadísticos

| Factor | Condición | Puntos |
|--------|-----------|--------|
| Precio bajo | Z-score < -1.5 | +30 |
| Anomalía extrema | Z-score < -2.5 | +40 |
| Precio bajo vs estimado | Precio < 40% del valor de mercado | +20 |

### Factores heurísticos

| Factor | Condición | Puntos |
|--------|-----------|--------|
| Descripción corta | < 30 caracteres con precio > 200€ | +15 |
| Contacto externo | Mención de WhatsApp, teléfono, email | +30 |
| Cuenta nueva | Fecha de registro reciente | Variable |
| Sin reseñas | Vendedor sin historial de ventas | Variable |

### Cálculo del Z-score compuesto

Se utiliza un Z-score ponderado que combina:

```
Z_compuesto = 0.5 × Z_cpu + 0.3 × Z_gpu + 0.1 × Z_ram + 0.1 × Z_categoría
```

Donde cada Z individual se calcula como:
```
Z = (precio_anuncio - media_mercado) / desviacion_estandar
```

Un Z-score negativo indica que el precio está por debajo de la media del mercado para productos similares.

---

## Visualización con Kibana

El proyecto incluye un dashboard preconfigurado que muestra:

- **Distribución temporal de anuncios**: Histograma de anuncios nuevos publicados o modificados
- **Mapa geográfico**: Ubicación de los anuncios
- **Top factores de riesgo**: Frecuencia de cada tipo de alerta
- **Tabla de detalle**: Lista filtrable de artículos de alto riesgo

### Importar el dashboard

```bash
# Mediante la API de Kibana
POST api/saved_objects/_import
# Subir el archivo kibana/dashboard_export.ndjson
```

O desde la interfaz: **Stack Management > Saved Objects > Import**

---

## Licencia

Este proyecto se distribuye bajo la licencia MIT. Consulta el archivo `LICENSE` para más detalles.

---

## Autores

- **Ana Montuenga García** - [GitHub](https://github.com/AnaMontuengaGarcia)
- **Daniel Modrego Solsona** - [GitHub](https://github.com/D-MSol4)

---

## Agradecimientos

- A la comunidad de Elasticsearch y ElastAlert por las herramientas de monitorización
- A los investigadores en detección de fraudes online por los patrones y heurísticas documentadas
