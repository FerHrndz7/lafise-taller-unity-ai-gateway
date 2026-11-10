# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicio 1 — Crear un Model Service gobernado (usage tracking + inference table)
# MAGIC
# MAGIC **Tiempo estimado:** 17 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Crear un **Model Service** del Unity AI Gateway que el banco *posee* y gobierna, **dentro de Unity Catalog**.
# MAGIC - Activar la **inference table** para que cada request/response quede registrado en UC.
# MAGIC - Usar el modelo para **clasificar casos** de clientes, igual que lo haría una app del Centro de Atención.
# MAGIC
# MAGIC ## Concepto clave
# MAGIC El **Unity AI Gateway** crea un **Model Service**: un securable de **Unity Catalog** (`catálogo.schema.nombre`)
# MAGIC que hereda permisos, lineage y auditoría de UC y enruta hacia uno o más foundation models. Para un modelo
# MAGIC **hosteado por Databricks (pay-per-token) no hace falta ninguna credencial**: nada de tokens personales ni de
# MAGIC secrets. Su configuración de gobierno tiene tres partes:
# MAGIC
# MAGIC | Parte | Para qué | Ejercicio |
# MAGIC |---|---|---|
# MAGIC | `routing` | A qué modelo(s) se envía cada llamada | 01 y 04 |
# MAGIC | `inference_table` | Registro de cada request/response en una tabla Delta | 01 y 03 |
# MAGIC | `rate_limits` | Tope de llamadas por periodo | 02 |
# MAGIC
# MAGIC ## ¿Por qué un Model Service propio?
# MAGIC Los modelos `system.ai.databricks-*` los administra Databricks. Al crear **tu** Model Service que enruta hacia
# MAGIC uno de ellos obtienes un objeto **que tu equipo gobierna**: quién lo puede usar, cuánto, con qué registro y con
# MAGIC qué modelo detrás. Las aplicaciones apuntan al **nombre del servicio**, no al modelo: si mañana el banco cambia
# MAGIC de modelo, las apps no cambian.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración — escribe tu catálogo en el widget `catalog`
# MAGIC
# MAGIC La primera vez el widget `catalog` (arriba del notebook) está **vacío** y esta celda se detiene con un
# MAGIC aviso. Escribe el catálogo del taller (el mismo de `00_setup`) y **vuelve a ejecutar**.

# COMMAND ----------

import re

dbutils.widgets.text("catalog", "", "Catálogo")
CATALOG = dbutils.widgets.get("catalog").strip()

user_email = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", user_email.split("@")[0].lower())
SCHEMA = f"ai_gateway_{user_prefix}"

if not CATALOG:
    _default = spark.sql("SELECT current_catalog()").collect()[0][0]
    raise ValueError(
        "✋ Escribe el catálogo del taller en el widget 'catalog' (arriba del notebook) y vuelve a ejecutar. "
        f"Usa el mismo de 00_setup (el catálogo por defecto de tu workspace es '{_default}')."
    )
if CATALOG in ("hive_metastore", "spark_catalog"):
    raise ValueError(f"'{CATALOG}' no es un catálogo de Unity Catalog. Escribe un catálogo UC en el widget 'catalog'.")
if "." in CATALOG:
    raise ValueError(f"El catálogo '{CATALOG}' contiene un punto; el Unity AI Gateway no lo admite. Usa otro catálogo.")

print(f"Usuario: {user_email} | Destino: {CATALOG}.{SCHEMA}")

# COMMAND ----------

import time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

SERVICE_NAME = f"lafise_atencion_{user_prefix}"     # nombre del Model Service (última parte del nombre UC)
SERVICE_FQN = f"{CATALOG}.{SCHEMA}.{SERVICE_NAME}"  # nombre de 3 partes en Unity Catalog
TABLE_PREFIX = f"{user_prefix}_atencion"            # la inference table será <prefijo>_payload
print(f"Model Service (UC): {SERVICE_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers del Unity AI Gateway
# MAGIC Llamamos a la **REST API** de Model Services con `w.api_client.do(...)`, que usa la **autenticación de tu
# MAGIC sesión** en el notebook (sin token ni secret).
# MAGIC
# MAGIC - Administración: `/api/2.1/unity-catalog/model-services`
# MAGIC - Consulta (formato compatible con OpenAI): `POST /ai-gateway/mlflow/v1/chat/completions` con `"model": "<catálogo.schema.servicio>"`
# MAGIC
# MAGIC > **Detalle de clúster clásico:** en un clúster clásico el SDK se conecta a la URL *regional* del control plane,
# MAGIC > donde la ruta `/ai-gateway` no existe (responde `Not Found`). Por eso `gw_chat()` envía la consulta a la
# MAGIC > **URL del workspace** con la misma autenticación de tu sesión. Las aplicaciones del banco siempre usan la URL del
# MAGIC > workspace, así que no les afecta.

# COMMAND ----------

MS_BASE = "/api/2.1/unity-catalog/model-services"
GW_PATH = "/ai-gateway/mlflow/v1/chat/completions"

import requests
def _workspace_url():
    # En un clúster clásico el SDK apunta a la URL regional del control plane, donde el gateway no responde:
    # las consultas van a la URL del workspace con la misma autenticación de la sesión (sin token ni secret).
    try:
        return "https://" + spark.conf.get("spark.databricks.workspaceUrl")
    except Exception:
        return w.config.host

GW_CHAT = _workspace_url().rstrip("/") + GW_PATH

def gw_chat(body):
    """POST al Unity AI Gateway (formato OpenAI chat completions)."""
    r = requests.post(GW_CHAT, headers=w.config.authenticate(), json=body, timeout=120)
    if r.status_code != 200:
        raise RuntimeError(f"{r.status_code} {r.text[:300]}")
    return r.json()

def ms_get(fqn):
    """Devuelve el Model Service o None si no existe."""
    try:
        return w.api_client.do("GET", f"{MS_BASE}/{fqn}")
    except Exception:
        return None

def ask_model(fqn, content, system=None, max_tokens=256):
    """Consulta el Model Service vía el gateway. La respuesta puede venir como texto o como lista de partes."""
    msgs = ([{"role": "system", "content": system}] if system else []) + [{"role": "user", "content": content}]
    resp = gw_chat({"model": fqn, "messages": msgs, "max_tokens": max_tokens})
    c = resp["choices"][0]["message"]["content"]
    if isinstance(c, list):  # algunos modelos (p. ej. Gemini) devuelven partes estructuradas
        return "".join(p.get("text", "") for p in c if isinstance(p, dict)).strip()
    return (c or "").strip()

def wait_queryable(fqn, timeout=180):
    """Un Model Service recién creado tarda unos segundos en aceptar consultas."""
    start, last = time.time(), None
    while time.time() - start < timeout:
        try:
            ask_model(fqn, "ping", max_tokens=16)
            return True
        except Exception as e:
            last = e
            time.sleep(6)
    raise TimeoutError(f"{fqn} no quedó consultable tras {timeout}s. Último error: {str(last)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Elegir el foundation model que vamos a gobernar
# MAGIC
# MAGIC Los nombres de los modelos **`system.ai.databricks-*`** (pay-per-token) cambian con el tiempo y Unity Catalog sigue
# MAGIC listando algunos ya retirados, así que no conviene dejarlos fijos en el código. Probamos en orden una lista corta
# MAGIC de modelos preferidos y nos quedamos con el primero que tiene pay-per-token **activo**.

# COMMAND ----------

def modelo_activo(nombre):
    """True si el modelo tiene pay-per-token activo (existe su endpoint databricks-*). Una sola consulta rápida."""
    try:
        w.serving_endpoints.get(nombre.removeprefix("system.ai."))
        return True
    except Exception:
        return False

def elegir_modelos(preferidos, n=1, excluir=()):
    """Primeros n modelos activos: primero los preferidos y, si no alcanzan, el resto de system.ai (uno por uno).
    No listamos todos los endpoints del workspace: en workspaces grandes esa llamada puede tardar demasiado."""
    elegidos = [p for p in preferidos if p not in excluir and modelo_activo(p)][:n]
    if len(elegidos) < n:
        resto = sorted(m["full_name"] for m in w.api_client.do(
            "GET", "/api/2.1/unity-catalog/models",
            query={"catalog_name": "system", "schema_name": "ai", "max_results": 400}).get("registered_models", [])
            if m["full_name"].startswith("system.ai.databricks-") and m["full_name"] not in preferidos)
        for m in resto:
            if len(elegidos) == n:
                break
            if m not in excluir and modelo_activo(m):
                elegidos.append(m)
    assert elegidos, "No hay foundation models pay-per-token activos en system.ai (revisa las Foundation Model APIs)"
    return elegidos

# Preferimos un modelo de chat sólido y económico; si no está activo, el siguiente.
PREFERRED = [
    "system.ai.databricks-claude-haiku-4-5",
    "system.ai.databricks-gpt-5-6-sol",
    "system.ai.databricks-gemini-3-5-flash",
    "system.ai.databricks-meta-llama-3-3-70b-instruct",
]
BASE_MODEL = elegir_modelos(PREFERRED)[0]
MODEL_REF = f"models/{BASE_MODEL}"
print(f"Modelo a gobernar: {BASE_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Crear el Model Service con inference table
# MAGIC
# MAGIC Observa los campos del destino: `destination_type = DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL` (modelo
# MAGIC hosteado por Databricks) y `traffic_percentage = 100` (todo el tráfico va a este único destino). La
# MAGIC `inference_table` escribe en **tu** schema.
# MAGIC
# MAGIC > Es **idempotente**: si el Model Service ya existe, lo reutiliza.

# COMMAND ----------

# El usage tracking es automático (system.ai_gateway.usage); aquí agregamos la inference table
# para registrar el contenido completo de cada llamada en Unity Catalog.
if ms_get(SERVICE_FQN):
    print(f"El Model Service {SERVICE_FQN} ya existe — reutilizándolo.")
else:
    # Si quedó una inference table de una corrida anterior (sin su Model Service), la borramos
    # para que la creación no choque con "table already exists".
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.{SCHEMA}.{TABLE_PREFIX}_payload")
    body = {
        "comment": "LAFISE - Centro de Atencion al Cliente (taller Unity AI Gateway)",
        "config": {
            "routing": {
                "destinations": [
                    {
                        "name": "primary",
                        "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                        "pay_per_token_config": {"model": MODEL_REF},
                        "traffic_percentage": 100,
                    }
                ]
            },
            "inference_table": {
                "parent": f"schemas/{CATALOG}.{SCHEMA}",
                "table_name_prefix": TABLE_PREFIX,
            },
        },
    }
    w.api_client.do(
        "POST", MS_BASE,
        query={"parent": f"schemas/{CATALOG}.{SCHEMA}", "model_service_id": SERVICE_NAME},
        body=body,
    )
    print(f"Creando {SERVICE_FQN} ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Esperar a que el Model Service acepte consultas

# COMMAND ----------

wait_queryable(SERVICE_FQN)
print("✅ El Model Service está listo para consultas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Clasificar casos de clientes con el modelo gobernado
# MAGIC
# MAGIC Simulamos lo que haría la app del Centro de Atención: enviamos 6 casos reales de la tabla (3 **con** datos
# MAGIC sensibles en el texto, uno de ellos con el número completo de una tarjeta, y 3 sin ellos) y pedimos la categoría. Estas llamadas quedarán en la inference table, y
# MAGIC en el Ejercicio 3 veremos qué datos de clientes viajaron al modelo.

# COMMAND ----------

from pyspark.sql import functions as F

CASOS_TABLE = f"`{CATALOG}`.{SCHEMA}.{user_prefix}_casos_clientes"
casos = spark.table(CASOS_TABLE)
con_tarjeta = F.col("mensaje").contains("número de mi tarjeta")
con_correo = F.col("mensaje").contains("@")
muestra = (casos.filter(con_tarjeta).orderBy("caso_id").limit(1).collect()
           + casos.filter(con_correo & ~con_tarjeta).orderBy("caso_id").limit(2).collect()
           + casos.filter(~con_correo).orderBy("caso_id").limit(3).collect())

SISTEMA = ("Eres analista del Centro de Atención al Cliente de un banco. Clasifica el caso en UNA de estas "
           "categorías: transferencias, tarjetas, banca_digital, cuentas, prestamos, remesas, atencion_cliente. "
           "Responde solo con la categoría.")
for row in muestra:
    out = ask_model(SERVICE_FQN, row.mensaje, system=SISTEMA, max_tokens=64)
    print(f"{row.caso_id} | real: {row.categoria:<16} | modelo: {out!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Ver el Model Service en la interfaz
# MAGIC
# MAGIC Abre **Catalog** ▸ tu catálogo ▸ `ai_gateway_<tu_usuario>`: junto a las tablas verás el Model Service
# MAGIC `lafise_atencion_<tu_usuario>`. También aparece en **AI Gateway** (menú lateral). Desde ahí se administran sus
# MAGIC **permisos**: quién puede consultarlo (por ejemplo, solo el grupo del Centro de Atención) y quién puede
# MAGIC cambiar su configuración, igual que con cualquier tabla de Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

# Resultado esperado:
#   ✅ ¡Bien! Model Service <CATALOG>.ai_gateway_<usuario>.lafise_atencion_<usuario> con inference table -> ...
try:
    svc = ms_get(SERVICE_FQN)
    assert svc is not None, "No se encontró el Model Service"
    it = (svc.get("config") or {}).get("inference_table") or {}
    assert it and not it.get("disabled", False), "La inference table no está habilitada"
    print(f"✅ ¡Bien! Model Service {SERVICE_FQN} con inference table -> {it.get('table')}")
except AssertionError as e:
    print("❌ Casi:", e)
    print("   Revisa el Paso 2: el bloque inference_table debe apuntar a schemas/CATALOG.SCHEMA.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del ejercicio
# MAGIC Creaste un **Model Service** gobernado en Unity Catalog (sin credenciales), activaste la **inference table** y lo
# MAGIC usaste para clasificar casos. El **usage tracking** es automático: cada llamada queda medida y registrada.
# MAGIC
# MAGIC ➡️ Siguiente: **02_rate_limits_y_guardrails** — limita el tráfico y conoce los guardrails.
# MAGIC
# MAGIC ## Glosario
# MAGIC | Término | Definición |
# MAGIC |---|---|
# MAGIC | **Unity AI Gateway** (antes Mosaic AI Gateway) | Capa de gobierno de IA de Databricks: crea **Model Services** en Unity Catalog con routing, inference tables, rate limits y service policies. |
# MAGIC | **Model Service** | Securable de Unity Catalog (`catálogo.schema.nombre`) que enruta a uno o más foundation models; hereda permisos, lineage y auditoría de UC. |
# MAGIC | **Foundation model** | Modelo de lenguaje grande de propósito general (Claude, GPT, Gemini, Llama) que Databricks ofrece listo para usar. |
# MAGIC | **Pay-per-token** | Modelo hosteado por Databricks que se factura por tokens; **no requiere credenciales**. |
# MAGIC | **Token** | Unidad de texto (~¾ de palabra) con la que se miden y cobran las llamadas a un modelo. |
# MAGIC | **Inference table** | Tabla Delta en UC donde el gateway registra cada request/response para auditoría. |
# MAGIC | **Usage tracking** | Medición automática del consumo de cada llamada (`system.ai_gateway.usage`). |
# MAGIC | **`system.ai`** | Schema de Unity Catalog con los foundation models registrados por Databricks. |
# MAGIC | **Securable** | Objeto de Unity Catalog al que se le pueden dar permisos (catálogo, schema, tabla, función, Model Service). |
