# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicio 2 — Rate limits y guardrails (service policies)
# MAGIC
# MAGIC **Tiempo estimado:** 12 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Aplicar un **rate limit** al Model Service para proteger el costo y la disponibilidad.
# MAGIC - **Ver el límite en acción**: qué recibe una aplicación cuando se pasa del tope.
# MAGIC - Entender cómo el Unity AI Gateway aplica **guardrails**: con **service policies** de Unity Catalog.
# MAGIC
# MAGIC ## Concepto clave
# MAGIC La configuración del Model Service se edita con un **PATCH** y un `update_mask` que dice qué parte cambia. Los
# MAGIC rate limits viven en `config.rate_limits`. Los **guardrails** (PII, contenido inseguro, temas válidos) **no**
# MAGIC son un campo del Model Service: son **service policies** de Unity Catalog que se adjuntan aparte.
# MAGIC
# MAGIC **¿Por qué importa en un banco?** Un script mal hecho o un agente en bucle puede consumir miles de llamadas en
# MAGIC minutos. El rate limit protege el presupuesto de cada equipo y evita que una aplicación deje sin servicio a las
# MAGIC demás que comparten el modelo.

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
SERVICE_FQN = f"{CATALOG}.{SCHEMA}.lafise_atencion_{user_prefix}"
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

def set_rate_limit(requests_per_minute):
    """PATCH solo de config.rate_limits: el resto de la configuración no se toca."""
    w.api_client.do(
        "PATCH", f"{MS_BASE}/{SERVICE_FQN}",
        query={"update_mask": "config.rate_limits"},
        body={"config": {"rate_limits": [
            {"key": "RATE_LIMIT_KEY_SERVICE",
             "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE",
             "requests": requests_per_minute}
        ]}},
    )

def ask_model(content, max_tokens=16):
    resp = gw_chat({
        "model": SERVICE_FQN, "messages": [{"role": "user", "content": content}], "max_tokens": max_tokens})
    return resp["choices"][0]["message"]["content"]

assert w.api_client.do("GET", f"{MS_BASE}/{SERVICE_FQN}"), "Primero ejecuta el Ejercicio 1"
print(f"Model Service: {SERVICE_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Aplicar un rate limit de producción
# MAGIC
# MAGIC `PATCH` con `update_mask=config.rate_limits`, usando `key = RATE_LIMIT_KEY_SERVICE` (un tope para todo el
# MAGIC servicio), `renewal_period = RATE_LIMIT_RENEWAL_PERIOD_MINUTE` y `requests = 60`.
# MAGIC
# MAGIC > Con `RATE_LIMIT_KEY_USER` (más un `principal`) el tope se aplica **por usuario o grupo**: útil para que un
# MAGIC > solo analista no consuma la cuota de todo su equipo.

# COMMAND ----------

set_rate_limit(60)
print("✅ Rate limit aplicado: 60 requests/min para todo el servicio.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Ver el límite en acción
# MAGIC
# MAGIC Bajamos el tope a **2 requests/min**, esperamos a que se propague y enviamos llamadas cada 3 segundos. Las que se
# MAGIC pasan del tope reciben un error **429 (REQUEST_LIMIT_EXCEEDED)**: es lo que vería una aplicación, que debe
# MAGIC reintentar más tarde. Al final **restauramos** el tope de 60/min.
# MAGIC
# MAGIC > El tope es aproximado (el gateway cuenta por ventanas de un minuto) y tarda unos segundos en propagarse: si no ves
# MAGIC > ningún 429, vuelve a ejecutar la celda.

# COMMAND ----------

set_rate_limit(2)
time.sleep(30)  # el nuevo límite tarda unos segundos en propagarse
rechazos = 0
for i in range(1, 16):  # hasta ~45 s más de llamadas; paramos al ver 2 rechazos
    try:
        ask_model(f"Responde solo OK ({i})")
        print(f"llamada {i:>2}: ✅ 200")
    except Exception as e:
        es_429 = "429" in str(e) or "LIMIT" in str(e).upper()
        rechazos += es_429
        print(f"llamada {i:>2}: " + ("⛔ 429 (rate limit)" if es_429 else f"❌ {str(e)[:80]}"))
        if rechazos == 2:
            break
    time.sleep(3)
set_rate_limit(60)
print("\nTope restaurado a 60 requests/min.", "" if rechazos else "(No hubo 429: el límite aún no se propagaba; vuelve a ejecutar.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Guardrails con service policies (concepto)
# MAGIC
# MAGIC En el Unity AI Gateway los **guardrails** son **service policies de Unity Catalog** que se crean aparte y se
# MAGIC **adjuntan** al Model Service. Revisan la **entrada** (lo que el usuario envía) y la **salida** (lo que el
# MAGIC modelo responde).
# MAGIC
# MAGIC > ⚠️ **Estado: Beta.** La adjunción de una service policy se hace **por la interfaz**, está detrás de un
# MAGIC > preview a nivel de cuenta y requiere permisos sobre el Model Service. Por eso este paso es **guiado por el
# MAGIC > facilitador**, no un ejercicio programático.
# MAGIC
# MAGIC **Tipos de guardrail disponibles:**
# MAGIC - **Built-in:** contenido inseguro (`system.ai.block_unsafe_content`), *jailbreak* (intentos de saltarse las
# MAGIC   reglas del modelo), detección de datos sensibles.
# MAGIC - **Personalizados:** una **función SQL de UC** (o un LLM como juez). Así se implementan reglas propias del banco.
# MAGIC
# MAGIC **Ejemplos para un banco:**
# MAGIC
# MAGIC | Riesgo | Guardrail |
# MAGIC |---|---|
# MAGIC | Un analista pega el número completo de una tarjeta en el prompt | Detección de datos sensibles: bloquear o enmascarar antes de llegar al modelo |
# MAGIC | El modelo responde con datos de otro cliente | Política sobre la salida |
# MAGIC | Un usuario intenta que el asistente dé asesoría de inversión no autorizada | Función de UC que restringe a temas válidos |
# MAGIC | Lenguaje ofensivo en casos de clientes molestos | Contenido inseguro (built-in) |
# MAGIC
# MAGIC **Dónde se adjunta (demo del facilitador):**
# MAGIC
# MAGIC     AI Gateway ▸ Models ▸ <tu Model Service> ▸ pestaña Policies (Beta) ▸ Attach policy
# MAGIC
# MAGIC En el Ejercicio 3 vas a **medir** cuántos datos sensibles llegaron hoy al modelo sin guardrail: ese número es
# MAGIC el argumento para activarlos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

# Resultado esperado:
#   ✅ Rate limit activo: 60 req/RATE_LIMIT_RENEWAL_PERIOD_MINUTE (RATE_LIMIT_KEY_SERVICE)
svc = w.api_client.do("GET", f"{MS_BASE}/{SERVICE_FQN}")
rls = (svc.get("config") or {}).get("rate_limits") or []
try:
    assert rls, "No se encontraron rate limits en el Model Service"
    r = rls[0]
    assert int(r.get("requests", 0)) == 60, f"El tope es {r.get('requests')}; vuelve a ejecutar set_rate_limit(60)"
    print(f"✅ Rate limit activo: {r.get('requests')} req/{r.get('renewal_period')} ({r.get('key')})")
except AssertionError as e:
    print("❌ Casi:", e, "\n   Revisa el Paso 1: el PATCH debe usar update_mask=config.rate_limits.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del ejercicio
# MAGIC Aplicaste un **rate limit** con un `PATCH`, viste el **429** que recibe una aplicación al pasarse del tope y
# MAGIC conociste los **guardrails** como **service policies** de UC (Beta, adjunción por la interfaz).
# MAGIC
# MAGIC ➡️ Siguiente: **03_auditar_inference_table** — quién preguntó qué, cuántos tokens costó y qué datos viajaron.
# MAGIC
# MAGIC ## Glosario
# MAGIC | Término | Definición |
# MAGIC |---|---|
# MAGIC | **Rate limit** | Tope de llamadas por periodo. En Model Services: `config.rate_limits`, clave `RATE_LIMIT_KEY_SERVICE` (todo el servicio) o `RATE_LIMIT_KEY_USER` (con `principal`). |
# MAGIC | **`update_mask`** | Parámetro del `PATCH` que indica qué parte de `config` se actualiza (p. ej. `config.rate_limits`). |
# MAGIC | **PATCH** | Operación REST que modifica solo una parte de un objeto existente. |
# MAGIC | **429 / REQUEST_LIMIT_EXCEEDED** | Respuesta del gateway cuando se supera el rate limit; la aplicación debe reintentar más tarde. |
# MAGIC | **Guardrail** | Control sobre la entrada o la salida del modelo: datos sensibles, contenido inseguro, *jailbreak*, temas válidos. |
# MAGIC | **Service policy** | Guardrail de Unity Catalog (built-in o función SQL / LLM como juez) que se adjunta a un Model Service. **Beta**, adjunción por la interfaz. |
# MAGIC | **Jailbreak** | Intento de un usuario de hacer que el modelo ignore sus reglas o instrucciones. |
# MAGIC | **PII** | Información de identificación personal: nombre, documento, teléfono, correo, número de tarjeta o cuenta. |
