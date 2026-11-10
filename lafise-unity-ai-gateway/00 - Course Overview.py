# Databricks notebook source
# MAGIC %md
# MAGIC # LAFISE — Taller de Gobierno de IA con el Unity AI Gateway
# MAGIC ## Cada llamada a un modelo: medida, registrada, limitada y resiliente
# MAGIC
# MAGIC Un taller práctico (1 h 30 min) para **gobernar el uso de modelos de IA** en el banco con el **Unity AI
# MAGIC Gateway**. Crearás un **Model Service** en Unity Catalog, registrarás cada llamada en una **inference table**,
# MAGIC aplicarás **rate limits**, conocerás los **guardrails** (service policies), auditarás el consumo y configurarás
# MAGIC **routing multiproveedor** con respaldo.
# MAGIC
# MAGIC El escenario es transversal a todo el banco: el **Centro de Atención al Cliente** quiere un modelo para
# MAGIC clasificar y resumir los casos que llegan por la app, la banca en línea, WhatsApp y las sucursales, y
# MAGIC **Riesgo, Cumplimiento y Seguridad de la Información** exigen que cada llamada quede bajo control. El mismo
# MAGIC patrón aplica a cualquier equipo (Riesgo, Crédito, Marketing, TI, Operaciones) que quiera usar IA.
# MAGIC
# MAGIC ## Objetivos
# MAGIC - Crear un Model Service gobernado (sin tokens ni secrets) con usage tracking e inference table
# MAGIC - Aplicar rate limits y entender los guardrails como service policies de Unity Catalog
# MAGIC - Auditar quién le preguntó qué al modelo, cuántos tokens costó y si viajaron datos sensibles
# MAGIC - Enrutar a más de un modelo con reparto de tráfico (respaldo nativo)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## A. Aviso
# MAGIC
# MAGIC <div style="border-left: 4px solid #f44336; background: #ffebee; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <strong style="display:block; color:#c62828; margin-bottom:6px; font-size: 1.1em;">Solo laboratorio · datos sintéticos</strong>
# MAGIC <div style="color:#333;">
# MAGIC <ul>
# MAGIC <li>Ejecuta esta práctica en un <strong>catálogo de desarrollo o sandbox</strong>, nunca en uno de producción. Cada participante crea su propio schema (<code>ai_gateway_&lt;usuario&gt;</code>), una tabla de casos de ejemplo y tres Model Services con prefijo <code>lafise_</code>.</li>
# MAGIC <li>Clientes, casos, tarjetas y cuentas son <strong>sintéticos</strong>. <strong>Nunca</strong> envíes PII real de clientes de LAFISE a un modelo en un laboratorio.</li>
# MAGIC <li>Las llamadas a los foundation models son pay-per-token (consumo mínimo: unos pocos miles de tokens por persona).</li>
# MAGIC </ul>
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## B. Recorrido de la sesión (1 h 30 min)
# MAGIC
# MAGIC <div style="max-width: 1200px; margin: 0 auto; font-family: sans-serif;">
# MAGIC <div style="background: #F9F7F4; border-radius: 10px; padding: 20px 24px; box-shadow: 0 2px 8px rgba(27,49,57,0.06);">
# MAGIC <style>.agenda-table td, .agenda-table th { font-size: 14pt !important; }</style>
# MAGIC <table class="agenda-table" style="width: 100%; border-collapse: collapse; line-height: 1.5;">
# MAGIC   <thead>
# MAGIC      <tr style="background: #1B5162; color: white;">
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 60px;">#</th>
# MAGIC       <th style="padding: 10px 14px; text-align: center; border: 1px solid #EEEDE9; width: 90px;">Tiempo</th>
# MAGIC       <th style="padding: 10px 14px; text-align: left; border: 1px solid #EEEDE9;">Notebook</th>
# MAGIC      </tr>
# MAGIC   </thead>
# MAGIC   <tbody>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">—</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">10 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Introducción del facilitador: por qué gobernar la IA y qué añade el Unity AI Gateway</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">00</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">8 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Preparación: tu schema y ~3,000 casos de clientes de ejemplo</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">01</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">17 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Model Service gobernado con inference table; clasificar casos con el modelo</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">02</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">12 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Rate limits y guardrails (service policies)</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">03</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">15 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Auditar la inference table: llamadas, tokens, usuarios y datos sensibles</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">04</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">13 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Multiproveedor y respaldo con routing nativo (90/10)</td></tr>
# MAGIC      <tr style="background: white;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">05</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">10 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Reto: un Model Service para Marketing, de punta a punta (si no alcanza el tiempo, queda de tarea)</td></tr>
# MAGIC      <tr style="background: #FBFAF8;"><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center; font-weight:700; color:#1B5162;">—</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9; text-align:center;">5 min</td><td style="padding: 8px 14px; border: 1px solid #EEEDE9;">Cierre y preguntas</td></tr>
# MAGIC   </tbody>
# MAGIC </table>
# MAGIC </div>
# MAGIC </div>
# MAGIC
# MAGIC Empieza en `Laboratorio - Unity AI Gateway/00_setup`. Los notebooks `01`–`04` traen el **código completo**:
# MAGIC ejecútalos en orden y lee las explicaciones. El único con espacios por completar es `05_reto` (su solución está
# MAGIC en `solutions/`).

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## C. Requisitos
# MAGIC
# MAGIC <div style="border-left: 4px solid #1976d2; background: #e3f2fd; padding: 14px 18px; border-radius: 4px; margin: 16px 0;">
# MAGIC <div style="color:#333;">
# MAGIC
# MAGIC - Conecta un **clúster clásico** (DBR **17.3 LTS**, modo de acceso *Standard* o *Dedicated*) a cada notebook.
# MAGIC   Si tu workspace tiene Serverless, también funciona.
# MAGIC - **Catálogo:** en **cada notebook** la primera ejecución se detiene porque el widget `catalog` (arriba del
# MAGIC   notebook) empieza vacío. Escribe el catálogo del taller (el que validaron los organizadores) y vuelve a ejecutar.
# MAGIC - En ese catálogo: `USE CATALOG` y `CREATE SCHEMA` (en tu schema: `USE SCHEMA`, `CREATE TABLE`, `CREATE SERVICE`),
# MAGIC   más `EXECUTE` sobre los modelos `system.ai.*`.
# MAGIC - Foundation Model APIs (pay-per-token) habilitadas, en una región con Unity AI Gateway.
# MAGIC - Salida a PyPI para `%pip install` (si está bloqueada, instala `requirements.txt` como librerías del clúster).
# MAGIC - Todo se valida antes de la sesión con el notebook de validación de entorno.
# MAGIC
# MAGIC </div>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Limpieza al terminar (opcional)
# MAGIC
# MAGIC Para borrar todo lo que creó el taller, ejecuta esto en una celda nueva al final de `05_reto`
# MAGIC (ahí ya están definidos `CATALOG`, `SCHEMA` y `user_prefix`):
# MAGIC
# MAGIC     from databricks.sdk import WorkspaceClient
# MAGIC     w = WorkspaceClient()
# MAGIC     for n in ["lafise_atencion_", "lafise_atencion_multi_", "lafise_mkt_"]:
# MAGIC         try: w.api_client.do("DELETE", f"/api/2.1/unity-catalog/model-services/{CATALOG}.{SCHEMA}.{n}{user_prefix}")
# MAGIC         except Exception as e: print(n, e)
# MAGIC     spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA} CASCADE")

# COMMAND ----------

# MAGIC %md
# MAGIC &copy; 2026 Databricks, Inc. Todos los derechos reservados.
