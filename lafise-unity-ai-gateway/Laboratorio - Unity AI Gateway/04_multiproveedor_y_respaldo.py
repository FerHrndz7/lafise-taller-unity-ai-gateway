# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicio 4 — Multiproveedor y respaldo con routing nativo
# MAGIC
# MAGIC **Tiempo estimado:** 13 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Crear un Model Service enrutado a **dos** foundation models de proveedores distintos, con **reparto de tráfico**.
# MAGIC - Entender el **respaldo (fallback)** nativo del Unity AI Gateway y por qué importa para la continuidad del negocio.
# MAGIC
# MAGIC ## Concepto clave
# MAGIC `config.routing.destinations` acepta varios destinos, cada uno con `traffic_percentage` (deben sumar 100). El
# MAGIC gateway reparte el tráfico y puede degradar hacia el otro destino. **No hace falta ningún endpoint externo ni
# MAGIC credencial**, y la aplicación sigue llamando al **mismo nombre** de servicio.
# MAGIC
# MAGIC **¿Por qué en un banco?** Depender de un único proveedor de modelos es un riesgo operativo y de concentración de
# MAGIC terceros. Con dos destinos el banco puede: probar un modelo nuevo con el 10% del tráfico antes de migrar
# MAGIC (*canary*), comparar calidad y costo con datos reales, y seguir operando si un proveedor falla.
# MAGIC
# MAGIC > **Importante:** el `routing` se define **al crear** el Model Service. Hoy `config.routing` **no se puede
# MAGIC > editar con un PATCH** (a diferencia de `config.rate_limits`). Por eso creamos un **segundo** Model Service,
# MAGIC > `lafise_atencion_multi_<usuario>`, que nace con **dos destinos**.

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
from collections import Counter
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

BASE_FQN = f"{CATALOG}.{SCHEMA}.lafise_atencion_{user_prefix}"   # del Ejercicio 1 (un solo destino)
MULTI_NAME = f"lafise_atencion_multi_{user_prefix}"               # nuevo, con dos destinos
MULTI_FQN = f"{CATALOG}.{SCHEMA}.{MULTI_NAME}"

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
print(f"Model Service multiproveedor: {MULTI_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

def ms_get(fqn):
    try:
        return w.api_client.do("GET", f"{MS_BASE}/{fqn}")
    except Exception:
        return None

def ask_raw(fqn, content, max_tokens=64):
    """Devuelve la respuesta completa del gateway (incluye el campo 'model' con el modelo que respondió)."""
    return gw_chat({
        "model": fqn, "messages": [{"role": "user", "content": content}], "max_tokens": max_tokens})

def text_of(resp):
    c = resp["choices"][0]["message"]["content"]
    return "".join(p.get("text", "") for p in c if isinstance(p, dict)).strip() if isinstance(c, list) else (c or "").strip()

def wait_queryable(fqn, timeout=180):
    start, last = time.time(), None
    while time.time() - start < timeout:
        try:
            ask_raw(fqn, "ping", max_tokens=16)
            return True
        except Exception as e:
            last = e
            time.sleep(6)
    raise TimeoutError(f"{fqn} no quedó consultable tras {timeout}s. Último error: {str(last)[:300]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Elegir el modelo primario y uno de respaldo
# MAGIC
# MAGIC El **primario** es el modelo del Ejercicio 1. El **respaldo** es otro `system.ai.databricks-*` activo,
# MAGIC idealmente de **otro proveedor**.

# COMMAND ----------

base = ms_get(BASE_FQN)
base_dests = ((base or {}).get("config") or {}).get("routing", {}).get("destinations", [])
PRIMARY_MODEL = base_dests[0]["pay_per_token_config"]["model"] if base_dests else None

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

# Respaldo preferido de otro proveedor que el primario; si no está activo, el siguiente.
PREFERRED = ["system.ai.databricks-gpt-5-6-sol", "system.ai.databricks-gemini-3-5-flash",
             "system.ai.databricks-claude-haiku-4-5", "system.ai.databricks-meta-llama-3-3-70b-instruct"]
if not PRIMARY_MODEL:  # si no se hizo el Ejercicio 1
    PRIMARY_MODEL = f"models/{elegir_modelos(PREFERRED[2:] + PREFERRED[:2])[0]}"
SECONDARY_MODEL = f"models/{elegir_modelos(PREFERRED, excluir=(PRIMARY_MODEL.removeprefix('models/'),))[0]}"
print("Modelo primario:", PRIMARY_MODEL)
print("Modelo respaldo:", SECONDARY_MODEL)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Crear el Model Service con DOS destinos
# MAGIC
# MAGIC El primario recibe `traffic_percentage = 90` y el respaldo `10` (deben sumar 100).
# MAGIC
# MAGIC > Es **idempotente**: si el Model Service ya existe, lo reutiliza.

# COMMAND ----------

if ms_get(MULTI_FQN):
    print(f"El Model Service {MULTI_FQN} ya existe — reutilizándolo.")
else:
    w.api_client.do(
        "POST", MS_BASE,
        query={"parent": f"schemas/{CATALOG}.{SCHEMA}", "model_service_id": MULTI_NAME},
        body={
            "comment": "LAFISE - Atencion al Cliente multiproveedor (taller Unity AI Gateway)",
            "config": {"routing": {"destinations": [
                {"name": "primary",  "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                 "pay_per_token_config": {"model": PRIMARY_MODEL},   "traffic_percentage": 90},
                {"name": "fallback", "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                 "pay_per_token_config": {"model": SECONDARY_MODEL}, "traffic_percentage": 10},
            ]}},
        },
    )
    print(f"Creando {MULTI_FQN} con 2 destinos (90/10) ...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Enviar tráfico y ver el reparto
# MAGIC
# MAGIC Enviamos 10 resúmenes de casos al **mismo nombre** de servicio. El campo `model` de cada respuesta dice qué
# MAGIC modelo atendió la llamada: la mayoría irá al primario y, con suerte, alguna al respaldo (con 10 llamadas el
# MAGIC 90/10 es aproximado). La aplicación no tuvo que saber nada de esto.

# COMMAND ----------

wait_queryable(MULTI_FQN)
casos = spark.table(f"`{CATALOG}`.{SCHEMA}.{user_prefix}_casos_clientes").select("caso_id", "mensaje").limit(10).collect()
atendido_por = Counter()
for row in casos:
    resp = ask_raw(MULTI_FQN, f"Resume en una sola línea, sin formato ni títulos, este caso de un cliente del banco: {row.mensaje}", max_tokens=256)
    atendido_por[resp.get("model", "?")] += 1
    print(f"{row.caso_id}: {text_of(resp)[:90]}")
print("\nLlamadas por modelo:", dict(atendido_por))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

# Resultado esperado:
#   ✅ Routing con 2 destinos: [('primary', 90), ('fallback', 10)]
svc = ms_get(MULTI_FQN)
dests = [(d.get("name"), int(d.get("traffic_percentage", 0)))
         for d in (svc.get("config") or {}).get("routing", {}).get("destinations", [])
         if not d.get("is_deleted")]
try:
    assert len(dests) >= 2, "Se esperaban al menos 2 destinos"
    assert sum(p for _, p in dests) == 100, "Los porcentajes de tráfico deben sumar 100"
    print("✅ Routing con 2 destinos:", dests)
except AssertionError as e:
    print("❌ Casi:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del ejercicio
# MAGIC Configuraste un Model Service con **routing multiproveedor** y reparto de tráfico: el respaldo es nativo, sin
# MAGIC endpoints externos ni credenciales, y la aplicación sigue llamando al mismo nombre. Recuerda: el `routing` se
# MAGIC define **al crear** el servicio. Con los Ejercicios 1–3, el banco tiene un servicio de IA medido, registrado,
# MAGIC limitado y resiliente.
# MAGIC
# MAGIC ➡️ Siguiente: **05_reto** — aplica todo a un Model Service nuevo para Marketing.
# MAGIC
# MAGIC ## Glosario
# MAGIC | Término | Definición |
# MAGIC |---|---|
# MAGIC | **`routing.destinations`** | Lista de destinos del Model Service, cada uno con `traffic_percentage`. Se define **al crear** el servicio. |
# MAGIC | **`traffic_percentage`** | Porcentaje del tráfico que va a cada destino; el conjunto debe sumar 100. |
# MAGIC | **Fallback / respaldo** | Destino alternativo hacia el que el gateway puede degradar si el primario falla. |
# MAGIC | **Multiproveedor** | Servir con modelos de distintos proveedores (Anthropic, OpenAI, Google, Meta) detrás del mismo nombre. |
# MAGIC | **Canary** | Enviar un porcentaje pequeño del tráfico a un modelo nuevo para probarlo antes de migrar. |
# MAGIC | **Riesgo de concentración** | Depender de un único proveedor tercero para un servicio crítico. |
