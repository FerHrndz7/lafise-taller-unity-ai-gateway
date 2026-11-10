# Databricks notebook source
# MAGIC %md
# MAGIC # Preparación — Gobierno de IA con el Unity AI Gateway
# MAGIC
# MAGIC | | |
# MAGIC |---|---|
# MAGIC | **Duración** | ~75 min de práctica (esta preparación: ~8 min) |
# MAGIC | **Nivel** | Principiante |
# MAGIC | **Lenguaje** | Python + SQL |
# MAGIC | **Cómputo** | Clúster **clásico** (DBR **17.3 LTS**, modo *Standard* o *Dedicated*) |
# MAGIC | **Catalog.Schema** | `<catálogo_del_taller>.ai_gateway_<tu_usuario>` (el catálogo se escribe en el widget `catalog`) |
# MAGIC
# MAGIC ## Qué aprenderás hoy
# MAGIC 1. Qué es el **Unity AI Gateway** y por qué es parte del gobierno de datos e IA con Unity Catalog.
# MAGIC 2. Crear un **Model Service** gobernado en Unity Catalog (sin tokens ni secrets), con **usage tracking + inference table**.
# MAGIC 3. Aplicar **rate limits** y conocer los **guardrails** (service policies: PII, contenido inseguro, temas válidos).
# MAGIC 4. Auditar en la **inference table** quién le preguntó qué al modelo, cuántos tokens costó y si viajaron datos sensibles.
# MAGIC 5. Configurar **routing multiproveedor** con respaldo, sin cambiar el código de quien consume el modelo.
# MAGIC
# MAGIC Recorrido del laboratorio:
# MAGIC 1. `00_setup` — tu schema y los casos de ejemplo (**estás aquí**).
# MAGIC 2. `01_model_service_gobernado` — Model Service + inference table; clasificar casos con el modelo.
# MAGIC 3. `02_rate_limits_y_guardrails` — rate limit por servicio y guardrails como service policies.
# MAGIC 4. `03_auditar_inference_table` — llamadas, tokens, usuarios y datos sensibles en los prompts.
# MAGIC 5. `04_multiproveedor_y_respaldo` — dos modelos con reparto de tráfico 90/10.
# MAGIC 6. `05_reto` — un Model Service para el equipo de Marketing, de punta a punta.
# MAGIC
# MAGIC ## El escenario de negocio 🏦
# MAGIC El **Centro de Atención al Cliente** de LAFISE recibe miles de casos en español desde la app LAFISE Móvil, la
# MAGIC banca en línea, WhatsApp, el call center y las sucursales de la región. Una **actualización de la app móvil**
# MAGIC provocó fallas en **transferencias** y acaba de disparar los reclamos en Nicaragua. El equipo quiere un LLM
# MAGIC para **clasificar y resumir** los casos, pero **Riesgo, Cumplimiento y Seguridad de la Información** exigen que
# MAGIC cada llamada al modelo quede **limitada, registrada, auditable y sin exponer datos de clientes**.
# MAGIC Esa capa de control es el **Unity AI Gateway**, y sirve igual para cualquier otro equipo del banco.
# MAGIC
# MAGIC > ⚠️ **Datos sintéticos.** Clientes, cuentas y tarjetas son inventados. Nunca uses PII real en un laboratorio.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 1 — Dependencias
# MAGIC `faker` genera los casos sintéticos (es la única librería que se instala; el `databricks-sdk` que trae el
# MAGIC runtime ya alcanza para la API del Unity AI Gateway). Instalamos primero: el reinicio de Python borra las
# MAGIC variables, así que la configuración va después.

# COMMAND ----------

# MAGIC %pip install --retries 5 --timeout 300 --quiet "faker>=25.0.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 2 — Configuración: escribe tu catálogo en el widget `catalog`
# MAGIC
# MAGIC La primera vez el widget `catalog` (arriba del notebook) está **vacío** y esta celda se detiene con un
# MAGIC aviso. Escribe el catálogo del taller (el que validaron los organizadores) y **vuelve a ejecutar**.
# MAGIC Cada participante trabaja en su propio schema `ai_gateway_<tu_usuario>`.

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
        f"Pregunta al facilitador cuál es (el catálogo por defecto de tu workspace es '{_default}')."
    )
if CATALOG in ("hive_metastore", "spark_catalog"):
    raise ValueError(f"'{CATALOG}' no es un catálogo de Unity Catalog. Escribe un catálogo UC en el widget 'catalog'.")
if "." in CATALOG:
    raise ValueError(f"El catálogo '{CATALOG}' contiene un punto; el Unity AI Gateway no lo admite. Usa otro catálogo.")

print(f"Usuario: {user_email} | Destino: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 3 — Verificación del entorno
# MAGIC
# MAGIC Confirmamos la versión de Python, el runtime del clúster y la versión del Databricks SDK.

# COMMAND ----------

import os, sys
import databricks.sdk
from databricks.sdk import WorkspaceClient

print(f"Python        : {sys.version.split()[0]}")
assert sys.version_info >= (3, 10), "Se requiere Python 3.10+"
rt = os.environ.get("DATABRICKS_RUNTIME_VERSION", "?")
print(f"Cómputo       : {'Serverless' if rt.startswith('client') else f'clúster clásico, DBR {rt}'}")
print(f"databricks-sdk: {databricks.sdk.version.__version__}")

w = WorkspaceClient()
print("✅ El entorno se ve bien.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 4 — Validación de permisos
# MAGIC
# MAGIC Fallamos rápido y con un mensaje claro si no puedes crear objetos en el catálogo del taller.

# COMMAND ----------

try:
    spark.sql(f"USE CATALOG `{CATALOG}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.{SCHEMA}")
    spark.sql(f"CREATE TABLE IF NOT EXISTS `{CATALOG}`.{SCHEMA}.{user_prefix}_perm_check (x INT)")
    spark.sql(f"DROP TABLE IF EXISTS `{CATALOG}`.{SCHEMA}.{user_prefix}_perm_check")
    print(f"✅ Puedes crear objetos en {CATALOG}.{SCHEMA}")
except Exception as e:
    raise PermissionError(
        f"No se pueden crear objetos en {CATALOG}.{SCHEMA}. Pide a tu admin USE CATALOG + CREATE SCHEMA sobre "
        f"{CATALOG} (o escribe otro catálogo en el widget 'catalog'). Error: {e}"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 5 — Casos de clientes de ejemplo
# MAGIC
# MAGIC Llamamos al generador `./data/generar_casos`, que escribe `{user_prefix}_casos_clientes`: ~3,000 casos en
# MAGIC español de 5 países, con el pico de reclamos de transferencias en Nicaragua y **datos sensibles incrustados**
# MAGIC en el texto libre (nombres, correos, teléfonos y números de tarjeta) — la materia prima para auditar y para
# MAGIC los guardrails.

# COMMAND ----------

# MAGIC %run ./data/generar_casos $num_rows=3000

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sección 6 — Resumen de verificación

# COMMAND ----------

CASOS_TABLE = f"{CATALOG}.{SCHEMA}.{user_prefix}_casos_clientes"

n = spark.table(CASOS_TABLE).count()
print("=" * 64)
print("PREPARACIÓN COMPLETA ✅")
print("=" * 64)
print(f"Catalog.Schema : {CATALOG}.{SCHEMA}")
print(f"user_prefix    : {user_prefix}")
print(f"Tabla de casos : {CASOS_TABLE}  ({n} filas)")
print(f"Model Service  : {CATALOG}.{SCHEMA}.lafise_atencion_{user_prefix}  (lo crearás en el notebook 01)")
print("=" * 64)
print("➡️  Continúa con 01_model_service_gobernado")

display(spark.table(CASOS_TABLE).limit(5))
