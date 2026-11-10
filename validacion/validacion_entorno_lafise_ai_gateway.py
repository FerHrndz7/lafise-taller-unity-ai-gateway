# Databricks notebook source
# MAGIC %md
# MAGIC # Validación de entorno — Taller de Gobierno de IA con el Unity AI Gateway
# MAGIC
# MAGIC Ejecuta este notebook en el **workspace de LAFISE donde se hará el taller**, con un **clúster clásico**
# MAGIC adjunto (DBR **17.3 LTS**, modo *Standard*), para confirmar que el entorno soporta todo el laboratorio.
# MAGIC
# MAGIC Prueba, con **tu propio usuario**, cada pieza que usan los notebooks del taller y reporta
# MAGIC **PASA / FALLA / ALTERNATIVA / REVISAR** por requisito. Al final **borra todo lo que creó**.
# MAGIC
# MAGIC | # | Qué se valida | Lo usa |
# MAGIC |---|---|---|
# MAGIC | 0 | Instalación de `faker` desde PyPI (el `databricks-sdk` del runtime alcanza) | `00_setup` |
# MAGIC | 1 | Clúster clásico y runtime (DBR 17.3 LTS) | Todos |
# MAGIC | 2 | Catálogo de Unity Catalog utilizable (no `hive_metastore`, sin puntos en el nombre) | Widget `catalog` |
# MAGIC | 3 | Crear schema y tabla con datos de Faker | `00_setup` |
# MAGIC | 4 | SQL con parámetros `IDENTIFIER(:tabla)` | `03` |
# MAGIC | 5 | Foundation models pay-per-token en `system.ai` (al menos 2 activos) | `01`, `04` |
# MAGIC | 6 | API de **Model Services** del Unity AI Gateway | `01`–`05` |
# MAGIC | 7 | Crear un Model Service con **inference table** | `01` |
# MAGIC | 8 | Consultar el modelo vía el gateway (`/ai-gateway/mlflow/v1/chat/completions`) | `01`–`05` |
# MAGIC | 9 | Aplicar un **rate limit** con PATCH | `02` |
# MAGIC | 10 | Crear un Model Service **multidestino** con rate limit al crear | `04`, `05` |
# MAGIC | 11 | La inference table recibe registros (asíncrono) | `03` |
# MAGIC | 12 | Acceso a `system.ai_gateway.usage` (opcional) | `03` |
# MAGIC
# MAGIC ## Instrucciones
# MAGIC 1. Adjunta un **clúster clásico** DBR 17.3 LTS (el mismo que usarán los participantes).
# MAGIC 2. En el widget **catalog** escribe el catálogo que usarán en el taller. Si lo dejas vacío, el notebook se
# MAGIC    detiene y te muestra los catálogos que puedes ver.
# MAGIC 3. **Run all** (~5–10 min). Comparte la **tabla resumen final** con el facilitador.
# MAGIC
# MAGIC > **Recomendado:** ejecútalo con un usuario **sin permisos de admin**, con los mismos grupos que tendrán los
# MAGIC > participantes. Así se detectan permisos faltantes antes del día del taller.
# MAGIC >
# MAGIC > Si la **primera celda** (`%pip install`) falla o se queda colgada, el workspace no tiene salida a PyPI: instalen
# MAGIC > `faker>=25.0.0` como **librerías del clúster** (desde un mirror interno) y vuelvan a
# MAGIC > ejecutar.

# COMMAND ----------

# MAGIC %pip install --retries 3 --timeout 120 --quiet "faker>=25.0.0"
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Catálogo (requerido)")
dbutils.widgets.text("wait_minutes", "8", "Minutos de espera inference table")

import os, re, sys, time
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
usuario = spark.sql("SELECT current_user()").collect()[0][0]
user_prefix = re.sub(r"[^a-z0-9]", "_", usuario.split("@")[0].lower())
CATALOG = dbutils.widgets.get("catalog").strip()
if not CATALOG:
    _default = spark.sql("SELECT current_catalog()").collect()[0][0]
    try:
        _visibles = ", ".join(r[0] for r in spark.sql("SHOW CATALOGS").collect()
                              if r[0] not in ("system", "samples", "hive_metastore"))
    except Exception:
        _visibles = "(no se pudieron listar)"
    raise ValueError("✋ Escribe en el widget 'catalog' el catálogo del taller y vuelve a ejecutar Run all. "
                     f"Catálogo por defecto: '{_default}'. Catálogos que puedes ver: {_visibles}")

SCHEMA = f"ai_gw_validacion_{user_prefix}"       # schema temporal, se borra al final
FQ = f"`{CATALOG}`.{SCHEMA}"
SVC_SINGLE, SVC_MULTI = f"validacion_gw_{user_prefix}", f"validacion_gw_multi_{user_prefix}"
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
PREFERRED = ["system.ai.databricks-claude-haiku-4-5", "system.ai.databricks-gpt-5-6-sol",
             "system.ai.databricks-gemini-3-5-flash", "system.ai.databricks-meta-llama-3-3-70b-instruct"]

resultados, estado = {}, {}

def marcar(req, est, detalle=""):
    resultados[req] = (est, str(detalle).strip().splitlines()[0][:300] if str(detalle).strip() else "")
    print(f"[{est}] {req} — {resultados[req][1]}")

def ms_get(fqn):
    try:
        return w.api_client.do("GET", f"{MS_BASE}/{fqn}")
    except Exception:
        return None

def ask_model(fqn, content, max_tokens=32):
    resp = gw_chat({
        "model": fqn, "messages": [{"role": "user", "content": content}], "max_tokens": max_tokens})
    c = resp["choices"][0]["message"]["content"]
    return "".join(p.get("text", "") for p in c if isinstance(p, dict)).strip() if isinstance(c, list) else (c or "").strip()

def wait_queryable(fqn, timeout=180):
    start, last = time.time(), None
    while time.time() - start < timeout:
        try:
            return ask_model(fqn, "Responde solo: OK")
        except Exception as e:
            last = e
            time.sleep(6)
    raise TimeoutError(f"{fqn} no respondió en {timeout}s. Último error: {str(last)[:300]}")

import databricks.sdk, faker
marcar("0. Librerías instaladas desde PyPI", "PASA",
       f"databricks-sdk {databricks.sdk.version.__version__}, faker {faker.VERSION}, Python {sys.version.split()[0]}")
print(f"Usuario: {usuario} | Catálogo: {CATALOG} | Schema temporal: {CATALOG}.{SCHEMA}")

# COMMAND ----------

rt = os.environ.get("DATABRICKS_RUNTIME_VERSION", "?")
if rt.startswith("client"):
    marcar("1. Clúster clásico y runtime", "ALTERNATIVA",
           "Estás en Serverless: el taller también funciona, pero valídalo en el clúster clásico que usarán")
else:
    marcar("1. Clúster clásico y runtime", "PASA" if rt.startswith("17.") else "ALTERNATIVA",
           f"DBR {rt}" + ("" if rt.startswith("17.") else " (se recomienda 17.3 LTS)"))

# COMMAND ----------

try:
    assert CATALOG not in ("hive_metastore", "spark_catalog"), f"'{CATALOG}' no es un catálogo de Unity Catalog"
    assert "." not in CATALOG, f"el nombre '{CATALOG}' contiene un punto (no soportado por Model Services)"
    spark.sql(f"USE CATALOG `{CATALOG}`")
    estado["catalog"] = True
    marcar("2. Catálogo de Unity Catalog", "PASA", f"'{CATALOG}' es utilizable")
except Exception as e:
    marcar("2. Catálogo de Unity Catalog", "FALLA", e)

# COMMAND ----------

try:
    assert estado.get("catalog"), "depende del punto 2"
    from faker import Faker
    fake = Faker("es_MX")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
    estado["schema"] = True
    spark.createDataFrame([(i, fake.name(), fake.email()) for i in range(50)], ["id", "nombre", "correo"]) \
        .write.mode("overwrite").saveAsTable(f"{FQ}.casos_prueba")
    n = spark.table(f"{FQ}.casos_prueba").count()
    assert n == 50, f"se esperaban 50 filas, hay {n}"
    estado["table"] = f"{FQ}.casos_prueba"
    marcar("3. Crear schema y tabla (Faker)", "PASA", f"CREATE SCHEMA + CREATE TABLE OK en {CATALOG}.{SCHEMA}")
except Exception as e:
    marcar("3. Crear schema y tabla (Faker)", "FALLA",
           f"{e} · Pide USE CATALOG + CREATE SCHEMA sobre el catálogo (y CREATE TABLE en el schema)")

# COMMAND ----------

try:
    assert estado.get("table"), "depende del punto 3"
    n = spark.sql("SELECT COUNT(*) FROM IDENTIFIER(:t)", args={"t": estado["table"]}).collect()[0][0]
    marcar("4. SQL parametrizado IDENTIFIER()", "PASA", f"{n} filas")
except Exception as e:
    marcar("4. SQL parametrizado IDENTIFIER()", "FALLA", e)

# COMMAND ----------

try:
    models = w.api_client.do("GET", "/api/2.1/unity-catalog/models",
                             query={"catalog_name": "system", "schema_name": "ai", "max_results": 400}
                             ).get("registered_models", [])
    fm = sorted(m["full_name"] for m in models if m["full_name"].startswith("system.ai.databricks-"))
    ppt = {e.name for e in w.serving_endpoints.list() if e.name.startswith("databricks-")}
    activos = [n for n in fm if n.removeprefix("system.ai.") in ppt]
    assert activos, ("no hay modelos system.ai.databricks-* activos (¿Foundation Model APIs deshabilitadas o sin "
                     "USE SCHEMA / EXECUTE en system.ai?)")
    primary = next((p for p in PREFERRED if p in activos), activos[0])
    secondary = next((n for n in [p for p in PREFERRED if p in activos] + activos if n != primary), None)
    estado["primary"], estado["secondary"] = f"models/{primary}", (f"models/{secondary}" if secondary else None)
    if secondary:
        marcar("5. Foundation models (system.ai)", "PASA",
               f"{len(activos)} activos ({len(fm) - len(activos)} retirados ignorados); {primary} + {secondary}")
    else:
        marcar("5. Foundation models (system.ai)", "ALTERNATIVA",
               f"solo 1 modelo activo ({primary}); el Ejercicio 4 (multiproveedor) necesita 2")
except Exception as e:
    marcar("5. Foundation models (system.ai)", "FALLA", e)

# COMMAND ----------

try:
    assert estado.get("schema"), "depende del punto 3"
    r = w.api_client.do("GET", MS_BASE, query={"parent": f"schemas/{CATALOG}.{SCHEMA}"})
    estado["ms_api"] = True
    marcar("6. API de Model Services", "PASA", f"accesible ({len(r.get('model_services', []))} servicios en el schema)")
except Exception as e:
    marcar("6. API de Model Services", "FALLA",
           f"{e} · Confirmen que el Unity AI Gateway (Model Services) está disponible en la región del workspace")

# COMMAND ----------

try:
    assert estado.get("ms_api") and estado.get("primary"), "depende de los puntos 5 y 6"
    fqn = f"{CATALOG}.{SCHEMA}.{SVC_SINGLE}"
    if not ms_get(fqn):
        spark.sql(f"DROP TABLE IF EXISTS {FQ}.{user_prefix}_val_payload")
        w.api_client.do("POST", MS_BASE,
            query={"parent": f"schemas/{CATALOG}.{SCHEMA}", "model_service_id": SVC_SINGLE},
            body={"comment": "LAFISE validacion de entorno (se borra automaticamente)",
                  "config": {
                      "routing": {"destinations": [{
                          "name": "primary", "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                          "pay_per_token_config": {"model": estado["primary"]}, "traffic_percentage": 100}]},
                      "inference_table": {"parent": f"schemas/{CATALOG}.{SCHEMA}",
                                          "table_name_prefix": f"{user_prefix}_val"}}})
    estado["single"] = fqn
    it = (ms_get(fqn).get("config") or {}).get("inference_table") or {}
    estado["payload"] = (it.get("table") or f"{CATALOG}.{SCHEMA}.{user_prefix}_val_payload").removeprefix("tables/")
    marcar("7. Crear Model Service con inference table", "PASA", f"{fqn} -> {estado['payload']}")
except Exception as e:
    marcar("7. Crear Model Service con inference table", "FALLA",
           f"{e} · Pide CREATE SERVICE (y CREATE TABLE) en el schema, y EXECUTE sobre system.ai")

# COMMAND ----------

try:
    assert estado.get("single"), "depende del punto 7"
    out = wait_queryable(estado["single"])
    estado["query"] = True
    marcar("8. Consultar el modelo vía el gateway", "PASA", f"respuesta: {out[:40]!r}")
except Exception as e:
    marcar("8. Consultar el modelo vía el gateway", "FALLA", e)

# COMMAND ----------

try:
    assert estado.get("single"), "depende del punto 7"
    w.api_client.do("PATCH", f"{MS_BASE}/{estado['single']}", query={"update_mask": "config.rate_limits"},
                    body={"config": {"rate_limits": [{"key": "RATE_LIMIT_KEY_SERVICE",
                                                      "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE",
                                                      "requests": 60}]}})
    rls = (ms_get(estado["single"]).get("config") or {}).get("rate_limits") or []
    assert rls, "el PATCH no dejó rate limits en el servicio"
    marcar("9. Rate limit (PATCH)", "PASA", f"{rls[0].get('requests')} req/{rls[0].get('renewal_period')}")
except Exception as e:
    marcar("9. Rate limit (PATCH)", "FALLA", e)

# COMMAND ----------

try:
    assert estado.get("ms_api") and estado.get("secondary"), "depende de los puntos 5 (2 modelos) y 6"
    fqn = f"{CATALOG}.{SCHEMA}.{SVC_MULTI}"
    if not ms_get(fqn):
        w.api_client.do("POST", MS_BASE,
            query={"parent": f"schemas/{CATALOG}.{SCHEMA}", "model_service_id": SVC_MULTI},
            body={"comment": "LAFISE validacion multi (se borra automaticamente)",
                  "config": {
                      "routing": {"destinations": [
                          {"name": "primary", "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                           "pay_per_token_config": {"model": estado["primary"]}, "traffic_percentage": 90},
                          {"name": "fallback", "destination_type": "DESTINATION_TYPE_PAY_PER_TOKEN_FOUNDATION_MODEL",
                           "pay_per_token_config": {"model": estado["secondary"]}, "traffic_percentage": 10}]},
                      "rate_limits": [{"key": "RATE_LIMIT_KEY_SERVICE",
                                       "renewal_period": "RATE_LIMIT_RENEWAL_PERIOD_MINUTE", "requests": 30}]}})
    estado["multi"] = fqn
    out = wait_queryable(fqn)
    dests = [d.get("name") for d in (ms_get(fqn).get("config") or {}).get("routing", {}).get("destinations", [])
             if not d.get("is_deleted")]
    assert len(dests) >= 2, f"se esperaban 2 destinos, hay {dests}"
    marcar("10. Model Service multidestino", "PASA", f"destinos {dests} (90/10), respuesta {out[:30]!r}")
except Exception as e:
    marcar("10. Model Service multidestino", "FALLA" if estado.get("secondary") else "ALTERNATIVA", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. La inference table recibe registros
# MAGIC El registro es **asíncrono**; esperamos hasta `wait_minutes` (widget). Si no llega a tiempo queda **REVISAR**:
# MAGIC en el taller hay más tiempo entre el Ejercicio 1 y el 3.

# COMMAND ----------

try:
    assert estado.get("query"), "depende del punto 8"
    for i in range(3):
        ask_model(estado["single"], f"Di el número {i}")
    deadline, n = time.time() + float(dbutils.widgets.get("wait_minutes")) * 60, 0
    while time.time() < deadline:
        if spark.catalog.tableExists(estado["payload"]):
            n = spark.table(estado["payload"]).count()
            if n > 0:
                break
        time.sleep(20)
    if n > 0:
        marcar("11. Inference table con registros", "PASA", f"{n} registros en {estado['payload']}")
    else:
        marcar("11. Inference table con registros", "REVISAR",
               f"sin registros tras {dbutils.widgets.get('wait_minutes')} min (el logging puede tardar más; no bloquea)")
except Exception as e:
    marcar("11. Inference table con registros", "FALLA", e)

# COMMAND ----------

try:
    spark.sql("SELECT * FROM system.ai_gateway.usage LIMIT 1").collect()
    marcar("12. system.ai_gateway.usage (opcional)", "PASA", "lectura OK")
except Exception as e:
    marcar("12. system.ai_gateway.usage (opcional)", "ALTERNATIVA",
           f"sin acceso ({str(e)[:100]}). Paso opcional del Ejercicio 3; un admin puede otorgar USE SCHEMA + SELECT "
           "sobre system.ai_gateway")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Limpieza y resumen

# COMMAND ----------

for key in ("multi", "single"):
    if estado.get(key):
        try:
            w.api_client.do("DELETE", f"{MS_BASE}/{estado[key]}")
            print("🧹 Model Service eliminado:", estado[key])
        except Exception as e:
            print("⚠️ No se pudo eliminar", estado[key], "->", str(e)[:200])
if estado.get("schema"):
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {FQ} CASCADE")
        print(f"🧹 Schema eliminado: {CATALOG}.{SCHEMA}")
    except Exception as e:
        print("⚠️ No se pudo eliminar el schema:", str(e)[:200])

print(f"\n{'Requisito':48s} {'Estado':12s} Detalle")
for req, (est, det) in resultados.items():
    print(f"{req:48s} {est:12s} {det[:110]}")

fallas = [r for r, (e, _) in resultados.items() if e == "FALLA"]
print("=" * 72)
if not fallas:
    print("✅ EL ENTORNO ESTÁ LISTO para el taller. En el widget 'catalog' de cada notebook escriban:", CATALOG)
else:
    print(f"❌ HAY {len(fallas)} BLOQUEANTE(S): {', '.join(fallas)}")
    print("   Permisos que pide el taller sobre el catálogo: USE CATALOG, CREATE SCHEMA (y en el schema de cada")
    print("   participante: USE SCHEMA, CREATE TABLE, CREATE SERVICE), más EXECUTE sobre los modelos system.ai.*")
print("=" * 72)
