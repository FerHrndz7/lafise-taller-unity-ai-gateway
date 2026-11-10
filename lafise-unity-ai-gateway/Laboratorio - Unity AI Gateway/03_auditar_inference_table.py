# Databricks notebook source
# MAGIC %md
# MAGIC # Ejercicio 3 — Auditar la inference table
# MAGIC
# MAGIC **Tiempo estimado:** 15 minutos
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Confirmar que la **inference table** del Model Service registra cada llamada.
# MAGIC - Responder las preguntas de **Auditoría y Finanzas**: cuántas llamadas, **quién** las hizo y cuántos **tokens** costaron.
# MAGIC - Responder la pregunta de **Seguridad de la Información**: ¿llegaron **datos sensibles** de clientes al modelo?
# MAGIC
# MAGIC ## Concepto clave
# MAGIC El Unity AI Gateway registra cada request/response del Model Service en una **tabla Delta** gobernada por UC.
# MAGIC El registro es **asíncrono** (puede tardar de 1 a 5 minutos). Columnas clave: `event_time`, `requester`,
# MAGIC `status_code`, `destination_model`, `request`, `response`.
# MAGIC
# MAGIC > **Importante:** la inference table guarda el **texto completo** de los prompts, así que contiene los mismos
# MAGIC > datos sensibles que se enviaron al modelo. Trátala como una tabla confidencial: permisos de UC, y si hace
# MAGIC > falta, *column masks* o *row filters*.

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

PAYLOAD_TABLE = f"`{CATALOG}`.{SCHEMA}.{user_prefix}_atencion_payload"
# Las celdas %sql leen :payload_table desde este widget.
dbutils.widgets.text("payload_table", PAYLOAD_TABLE, "Inference table")
print(f"Inference table: {PAYLOAD_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Confirmar que la tabla existe
# MAGIC
# MAGIC Si dice `False`, los registros aún no llegaron: el logging es asíncrono. Espera 2–5 minutos y vuelve a
# MAGIC ejecutar (mientras tanto puedes adelantar el Ejercicio 4).

# COMMAND ----------

exists = spark.catalog.tableExists(PAYLOAD_TABLE.replace("`", ""))
print("Inference table presente:", exists)
if exists:
    display(spark.table(PAYLOAD_TABLE).limit(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Inspeccionar el schema
# MAGIC
# MAGIC Observa: `event_time`, `requester` (quién llamó), `status_code` (200 = OK, 429 = rate limit),
# MAGIC `destination_name` (a qué destino del routing fue) y los JSON `request` / `response`.

# COMMAND ----------

spark.table(PAYLOAD_TABLE).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Llamadas por día, usuario y resultado
# MAGIC
# MAGIC La pregunta típica de Auditoría: ¿quién usó el modelo, cuándo y con qué resultado? Fíjate en las llamadas
# MAGIC **429** del Ejercicio 2: el gateway también registra las llamadas rechazadas.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   CAST(event_time AS DATE) AS fecha,
# MAGIC   requester                AS usuario,
# MAGIC   status_code,
# MAGIC   COUNT(*)                 AS llamadas
# MAGIC FROM IDENTIFIER(:payload_table)
# MAGIC GROUP BY ALL
# MAGIC ORDER BY fecha, usuario, status_code

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Gasto de tokens
# MAGIC
# MAGIC La columna `response` es JSON. En chat completions trae un objeto `usage` con `prompt_tokens`,
# MAGIC `completion_tokens` y `total_tokens`. Es la base para el **chargeback**: cobrar a cada área lo que consume.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   destination_name                                                     AS destino,
# MAGIC   COUNT(*)                                                             AS llamadas,
# MAGIC   SUM(CAST(get_json_object(response, '$.usage.prompt_tokens') AS INT))     AS tokens_entrada,
# MAGIC   SUM(CAST(get_json_object(response, '$.usage.completion_tokens') AS INT)) AS tokens_salida,
# MAGIC   SUM(CAST(get_json_object(response, '$.usage.total_tokens') AS INT))      AS tokens_totales
# MAGIC FROM IDENTIFIER(:payload_table)
# MAGIC WHERE status_code = 200
# MAGIC GROUP BY destination_name

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — ¿Llegaron datos sensibles al modelo?
# MAGIC
# MAGIC Buscamos en el texto de los prompts patrones de **correo**, **teléfono** y **número de tarjeta**. En el
# MAGIC Ejercicio 1 enviamos a propósito casos con datos de clientes, así que deberías ver coincidencias. Este es el
# MAGIC tipo de control que Seguridad de la Información puede programar como alerta, y la evidencia para activar un
# MAGIC **guardrail** de datos sensibles (Ejercicio 2).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   COUNT(*)                                                                         AS prompts,
# MAGIC   COUNT_IF(request RLIKE '[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+[.][a-z]{2,}')           AS con_correo,
# MAGIC   COUNT_IF(request RLIKE '[+][0-9]{1,3} ?[0-9]{3}')                                AS con_telefono,
# MAGIC   COUNT_IF(request RLIKE '[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}')                    AS con_tarjeta
# MAGIC FROM IDENTIFIER(:payload_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — La vista de cuenta: `system.ai_gateway.usage` (opcional)
# MAGIC
# MAGIC El usage tracking alimenta `system.ai_gateway.usage`, una tabla de sistema con el consumo de **todos** los
# MAGIC servicios del gateway en la cuenta (modelos, agentes, servidores MCP): la fuente de verdad para el chargeback
# MAGIC entre áreas. Trae `requester`, `service_name` y los tokens ya separados (`input_tokens`, `output_tokens`).
# MAGIC
# MAGIC > Las tablas de sistema se actualizan con **retraso** (de minutos a una hora o más): es probable que tus llamadas de
# MAGIC > hoy todavía no aparezcan. Por eso miramos los últimos 7 días. Requiere que un admin otorgue acceso a
# MAGIC > `system.ai_gateway`; si no lo tienes, verás un aviso y puedes seguir.

# COMMAND ----------

try:
    display(spark.sql("""
        SELECT service_type, service_name,
               COUNT(DISTINCT requester) AS usuarios,
               COUNT(*)                  AS llamadas,
               SUM(input_tokens)         AS tokens_entrada,
               SUM(output_tokens)        AS tokens_salida
        FROM system.ai_gateway.usage
        WHERE event_time >= current_date() - INTERVAL 7 DAYS
        GROUP BY ALL
        ORDER BY llamadas DESC
        LIMIT 15
    """))
except Exception as e:
    print("Puede que system.ai_gateway no esté habilitado u otorgado en este workspace:", str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validación

# COMMAND ----------

# Resultado esperado:
#   ✅ Se encontraron N llamadas registradas en <CATALOG>.ai_gateway_<usuario>.<usuario>_atencion_payload.
try:
    assert spark.catalog.tableExists(PAYLOAD_TABLE.replace("`", "")), \
        "La inference table aún no existe (el logging es asíncrono: espera unos minutos)"
    n = spark.table(PAYLOAD_TABLE).count()
    assert n > 0, "La tabla existe pero está vacía: envía algunas consultas más y espera"
    print(f"✅ Se encontraron {n} llamadas registradas en {PAYLOAD_TABLE}. Ya puedes auditar cada llamada al modelo.")
except AssertionError as e:
    print("❌ Casi:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del ejercicio
# MAGIC Auditaste la **inference table**: llamadas por usuario y resultado, gasto de **tokens** y **datos sensibles**
# MAGIC en los prompts. Viste `system.ai_gateway.usage` como la fuente de verdad de la cuenta para el chargeback.
# MAGIC
# MAGIC ➡️ Siguiente: **04_multiproveedor_y_respaldo** — enruta a más de un modelo con respaldo nativo.
# MAGIC
# MAGIC ## Glosario
# MAGIC | Término | Definición |
# MAGIC |---|---|
# MAGIC | **Inference table** | Tabla Delta (`<schema>.<prefijo>_payload`) donde el gateway registra cada request/response. |
# MAGIC | **`requester`** | Usuario o service principal que hizo la llamada: la base de la trazabilidad. |
# MAGIC | **`status_code`** | Resultado HTTP de la llamada: 200 = OK, 429 = rechazada por rate limit. |
# MAGIC | **`IDENTIFIER(:param)`** | Usa el valor de un **widget** como nombre de tabla en una celda `%sql`. |
# MAGIC | **`get_json_object`** | Función SQL que extrae un campo (p. ej. `$.usage.total_tokens`) de una columna JSON. |
# MAGIC | **`RLIKE` / `COUNT_IF`** | Búsqueda por expresión regular / conteo condicional en SQL. |
# MAGIC | **Chargeback** | Asignar a cada área o equipo el costo de lo que consume. |
# MAGIC | **`system.ai_gateway.usage`** | Tabla de sistema con el consumo del gateway a nivel de cuenta. |
# MAGIC | **Column mask / row filter** | Reglas de UC que enmascaran columnas o filtran filas según quién consulta. |
