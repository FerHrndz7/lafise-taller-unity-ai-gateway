# Databricks notebook source
# MAGIC %md
# MAGIC # Generador de datos de ejemplo: casos del Centro de Atención al Cliente
# MAGIC
# MAGIC Genera **casos de clientes en español** realistas para los productos de un banco regional (tarjetas, cuentas,
# MAGIC préstamos, banca digital, transferencias y remesas) en los países donde opera LAFISE. Lo llama `00_setup`.
# MAGIC
# MAGIC **La historia:** a inicios de octubre una actualización de la **app LAFISE Móvil** provoca fallas en
# MAGIC **transferencias** en **Nicaragua**. Los reclamos se disparan durante 10 días, dominados por `transferencias` y
# MAGIC `banca_digital`. El texto libre trae **datos sensibles** (nombre, correo, teléfono y, a veces, el número completo
# MAGIC de la tarjeta) y algún lenguaje ofensivo — la materia prima para auditar y para los guardrails.
# MAGIC
# MAGIC Es **idempotente** (se puede re-ejecutar) y aísla los datos por participante con `user_prefix`.
# MAGIC
# MAGIC > **Nota:** ejecútalo siempre vía `%run` desde `00_setup`, que ya instaló `faker` y definió el catálogo.

# COMMAND ----------

dbutils.widgets.text("num_rows", "3000", "Número de casos")

# COMMAND ----------

NUM_ROWS = int(dbutils.widgets.get("num_rows"))

# CATALOG / SCHEMA / user_prefix vienen de la celda de configuración de 00_setup
# (un notebook ejecutado con %run comparte las variables del notebook que lo llama).
try:
    CATALOG, SCHEMA, user_prefix
except NameError:
    raise RuntimeError("Ejecuta este generador desde 00_setup (que define el catálogo), no por separado.")

CASOS_TABLE = f"`{CATALOG}`.{SCHEMA}.{user_prefix}_casos_clientes"
print(f"Generando {NUM_ROWS} casos -> {CASOS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generar los casos
# MAGIC
# MAGIC 3,000 filas se generan en segundos en el driver con Faker y se escriben como tabla Delta. Las distribuciones
# MAGIC son **sesgadas a propósito** (nunca uniformes) para que los datos cuenten una historia: el pico de
# MAGIC transferencias en Nicaragua, datos sensibles en ~35% de los mensajes y lenguaje ofensivo en los casos de menor
# MAGIC satisfacción.

# COMMAND ----------

import random
from datetime import datetime, timedelta
from faker import Faker

random.seed(42)
fake = Faker("es_MX")
Faker.seed(42)

PAISES = {  # país: (peso, prefijo telefónico, sucursal principal)
    "Nicaragua": (34, "+505 8", "Managua"),
    "Costa Rica": (20, "+506 8", "San José"),
    "Honduras": (18, "+504 9", "Tegucigalpa"),
    "Panamá": (16, "+507 6", "Ciudad de Panamá"),
    "República Dominicana": (12, "+1 809 ", "Santo Domingo"),
}
PRODUCTOS = {
    "tarjetas": ["tarjeta de crédito", "tarjeta de débito", "tarjeta de crédito Platinum"],
    "transferencias": ["transferencia ACH", "transferencia entre cuentas", "transferencia internacional"],
    "banca_digital": ["app LAFISE Móvil", "banca en línea", "token digital"],
    "cuentas": ["cuenta de ahorro", "cuenta corriente", "cuenta en dólares"],
    "prestamos": ["préstamo personal", "préstamo hipotecario", "préstamo de vehículo"],
    "remesas": ["remesa familiar", "envío de dinero"],
    "atencion_cliente": ["sucursal", "call center", "agencia"],
}
CATEGORIAS = (["transferencias"] * 22 + ["tarjetas"] * 22 + ["banca_digital"] * 18 + ["cuentas"] * 12
              + ["prestamos"] * 10 + ["remesas"] * 6 + ["atencion_cliente"] * 10)
CANALES = ["app_movil"] * 30 + ["whatsapp"] * 25 + ["call_center"] * 20 + ["banca_en_linea"] * 15 + ["sucursal"] * 10
MENSAJES = {
    "transferencias": [
        "Hice una {p} y el dinero salió de mi cuenta pero nunca llegó al destinatario.",
        "La {p} quedó en estado pendiente desde ayer, necesito que la reversen.",
        "Me cobraron dos veces la misma {p}, exijo la devolución.",
    ],
    "tarjetas": [
        "Tengo un cargo que no reconozco en mi {p}, no hice esa compra.",
        "Mi {p} fue bloqueada sin aviso y no puedo pagar en los comercios.",
        "Solicité un aumento de límite de la {p} hace un mes y no tengo respuesta.",
    ],
    "banca_digital": [
        "Después de la actualización la {p} se cierra sola y no me deja entrar.",
        "La {p} me da error al confirmar operaciones, no recibo el código.",
        "No puedo registrar un nuevo beneficiario en la {p}.",
    ],
    "cuentas": [
        "Quiero saber por qué me cobraron una comisión de manejo en mi {p}.",
        "Necesito un estado de cuenta de mi {p} de los últimos tres meses.",
    ],
    "prestamos": [
        "Pagué la cuota de mi {p} y no se refleja, me aparece en mora.",
        "Quisiera información para refinanciar mi {p}.",
    ],
    "remesas": [
        "Mi familia me envió una {p} y aún no la puedo cobrar.",
        "La {p} llegó con un monto menor al enviado.",
    ],
    "atencion_cliente": [
        "Esperé más de una hora en la {p} y no me atendieron.",
        "Nadie responde mis mensajes sobre el reclamo que puse en la {p}.",
    ],
}
SEGMENTOS = ["personas"] * 70 + ["pyme"] * 20 + ["corporativo"] * 10


def pick_pais():
    return random.choices(list(PAISES), weights=[v[0] for v in PAISES.values()])[0]


def tarjeta():
    return "4" + "".join(str(random.randint(0, 9)) for _ in range(3)) + "-" + "-".join(
        "".join(str(random.randint(0, 9)) for _ in range(4)) for _ in range(3))


INICIO_INCIDENTE, INICIO = datetime(2026, 10, 2), datetime(2026, 8, 1)
filas = []
for i in range(NUM_ROWS):
    # el incidente: transferencias / banca digital en Nicaragua, concentradas en 10 días de octubre
    incidente = random.random() < 0.18
    if incidente:
        categoria, pais = random.choice(["transferencias"] * 3 + ["banca_digital"] * 2), "Nicaragua"
        creado = INICIO_INCIDENTE + timedelta(days=random.randint(0, 9), hours=random.randint(0, 23))
        canal = random.choice(["app_movil"] * 3 + ["whatsapp", "call_center"])
    else:
        categoria, pais = random.choice(CATEGORIAS), pick_pais()
        creado = INICIO + timedelta(days=random.randint(0, 91), hours=random.randint(0, 23))
        canal = random.choice(CANALES)
    producto = random.choice(PRODUCTOS[categoria])
    nombre, correo = fake.name(), fake.email()
    telefono = PAISES[pais][1] + "".join(str(random.randint(0, 9)) for _ in range(7))
    satisfaccion = random.randint(1, 2) if incidente else random.randint(1, 5)
    msg = random.choice(MENSAJES[categoria]).format(p=producto)
    if random.random() < 0.35:  # datos sensibles incrustados en el texto libre
        msg += f" Soy {nombre}, mi teléfono es {telefono} y mi correo {correo}."
        if categoria == "tarjetas" and random.random() < 0.6:
            msg += f" El número de mi tarjeta es {tarjeta()}."
    if satisfaccion == 1 and random.random() < 0.25:  # material para guardrails
        msg += " Este banco es una estafa, son unos ladrones."
    filas.append((
        f"CAS-{i:06d}", pais, PAISES[pais][2], canal, categoria, producto, random.choice(SEGMENTOS),
        nombre, correo, telefono, satisfaccion,
        "alta" if satisfaccion <= 2 else ("media" if satisfaccion == 3 else "baja"), msg, creado,
    ))

COLUMNAS = ["caso_id", "pais", "sucursal", "canal", "categoria", "producto", "segmento", "cliente_nombre",
            "cliente_correo", "cliente_telefono", "satisfaccion", "prioridad", "mensaje", "creado_en"]
casos = spark.createDataFrame(filas, COLUMNAS)
casos.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CASOS_TABLE)
print(f"✅ Se escribieron {spark.table(CASOS_TABLE).count()} filas en {CASOS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Un vistazo rápido a la historia

# COMMAND ----------

display(spark.sql(f"""
    SELECT pais, categoria, COUNT(*) AS casos
    FROM {CASOS_TABLE}
    WHERE creado_en BETWEEN '2026-10-02' AND '2026-10-12'
    GROUP BY pais, categoria ORDER BY casos DESC LIMIT 8
"""))
