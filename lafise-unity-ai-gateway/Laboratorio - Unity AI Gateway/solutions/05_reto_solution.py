# Databricks notebook source
# MAGIC %md
# MAGIC # Reto (SOLUCIÓN) — Un Model Service gobernado de punta a punta
# MAGIC
# MAGIC **Tiempo estimado:** 10 minutos (si no alcanza en la sesión, queda de tarea)
# MAGIC
# MAGIC El equipo de **Marketing** de LAFISE quiere usar IA para redactar mensajes de campañas. Plataforma de Datos le
# MAGIC entrega su propio Model Service, separado del de Atención al Cliente, para que su consumo se mida y se limite
# MAGIC por separado. Créalo **en una sola llamada** con inference table **y** rate limit, y consúltalo.
# MAGIC
# MAGIC | # | Qué completas | Pista |
# MAGIC |---|---|---|
# MAGIC | 1 | El bloque `inference_table` | Igual que en el Ejercicio 1, con el prefijo `TABLE_PREFIX` |
# MAGIC | 2 | El tope de `rate_limits` | Marketing no necesita más de 30 requests/min |

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

MKT_NAME = f"lafise_mkt_{user_prefix}"
MKT_FQN = f"{CATALOG}.{SCHEMA}.{MKT_NAME}"
TABLE_PREFIX = f"{user_prefix}_mkt"
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
    try:
        return w.api_client.do("GET", f"{MS_BASE}/{fqn}")
    except Exception:
        return None

def ask_model(fqn, content, max_tokens=256):
    resp = gw_chat({"model": fqn, "messages": [{"role": "user", "content": content}], "max_tokens": max_tokens})
    c = resp["choices"][0]["message"]["content"]
    return "".join(p.get("text", "") for p in c if isinstance(p, dict)).strip() if isinstance(c, list) else (c or "").strip()

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

MODEL_REF = f"models/{elegir_modelos(['system.ai.databricks-claude-haiku-4-5', 'system.ai.databricks-gpt-5-6-sol'])[0]}"
print("Model Service destino:", MKT_FQN, "| modelo:", MODEL_REF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Crear el Model Service de Marketing (inference table + rate limit en un solo create)

# COMMAND ----------

# SOLUCIÓN: inference_table y rate_limits se pueden enviar en el mismo POST que crea el servicio.
if not ms_get(MKT_FQN):
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.{SCHEMA}.{TABLE_PREFIX}_payload")  # idempotencia
    w.api_client.do(
        "POST", MS_BASE,
        query={"parent": f"schemas/{CATALOG}.{SCHEMA}", "model_service_id": MKT_NAME},
        body={"comment": "LAFISE - Marketing (reto del taller Unity AI Gateway)",
              "config": {
                  "routing": {"destinations": [
                      {"name": "primary", "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                       "pay_per_token_config": {"model": MODEL_REF}, "traffic_percentage": 100}]},
                  # SOLUCIÓN 1
                  "inference_table": {"parent": f"schemas/{CATALOG}.{SCHEMA}", "table_name_prefix": TABLE_PREFIX},
                  # SOLUCIÓN 2
                  "rate_limits": [{"key": "RATE_LIMIT_KEY_SERVICE", "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 30}],
              }},
    )
    print("Creando", MKT_FQN, "...")
else:
    print("Ya existe:", MKT_FQN)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Consultarlo

# COMMAND ----------

start, respuesta = time.time(), None
while time.time() - start < 180 and respuesta is None:
    try:
        respuesta = ask_model(MKT_FQN, "Escribe un eslogan corto y cercano para la tarjeta de crédito de un banco "
                                       "regional centroamericano. Sin promesas de tasas ni montos. Responde solo con el eslogan, sin formato.")
    except Exception as e:
        if any(c in str(e)[:4] for c in ("401", "403")):
            raise PermissionError(f"Sin permiso para consultar {MKT_FQN}: {str(e)[:300]}")
        time.sleep(6)
print("Eslogan:", respuesta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

# Resultado esperado:
#   ✅ Reto completo: <CATALOG>.ai_gateway_<usuario>.lafise_mkt_<usuario> | inference_table: True | rate_limits: 30/min
svc = ms_get(MKT_FQN)
cfg = (svc or {}).get("config") or {}
rls = cfg.get("rate_limits") or []
ok = bool(svc and cfg.get("inference_table") and rls and respuesta)
print("✅ Reto completo:" if ok else "❌ Revisa la configuración:", MKT_FQN,
      "| inference_table:", bool(cfg.get("inference_table")),
      "| rate_limits:", f"{rls[0].get('requests')}/min" if rls else None)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen y alternativas
# MAGIC Creaste, gobernaste y consultaste un Model Service de punta a punta, sin credenciales y todo en Unity Catalog.
# MAGIC
# MAGIC **Otras formas válidas:**
# MAGIC - Crear el servicio solo con `routing` y agregar el rate limit después con un `PATCH` (Ejercicio 2).
# MAGIC   La inference table, en cambio, conviene definirla al crear.
# MAGIC - Usar `RATE_LIMIT_KEY_USER` con el grupo de Marketing como `principal`, para que el tope sea por usuario.
# MAGIC - Darle dos destinos (como en el Ejercicio 4) si Marketing quiere comparar modelos.
# MAGIC - En producción, crear el servicio con código versionado (Declarative Automation Bundles o Terraform) en vez de
# MAGIC   un notebook, y dar permiso de uso solo al grupo de Marketing.
