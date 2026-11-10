# Taller de Gobierno de IA con el Unity AI Gateway · LAFISE

Contenido práctico del taller de Gobierno de IA para Grupo LAFISE: cómo gobernar el uso de modelos de IA en el banco
con el **Unity AI Gateway** de Databricks. Cada participante crea un **Model Service** gobernado en Unity Catalog
(sin tokens ni secrets), registra y audita cada llamada en una **inference table**, aplica **rate limits**, conoce los
**guardrails** (service policies) y configura **routing multiproveedor** con respaldo.

El escenario es el Centro de Atención al Cliente clasificando y resumiendo casos de clientes de cinco países.

> Todos los datos del laboratorio son **100% sintéticos**. Nunca envíes PII real de clientes a un modelo en un
> workspace de laboratorio.

## Contenido

| Ruta | Qué es |
|---|---|
| `lafise-unity-ai-gateway/00 - Course Overview` | Agenda, requisitos y limpieza |
| `lafise-unity-ai-gateway/Laboratorio - Unity AI Gateway/` | Notebooks `00_setup` a `04` (código completo) y `05_reto`, con `solutions/` y `data/` |
| `lafise-unity-ai-gateway/requirements.txt` | Librería `faker` (para instalarla como librería de clúster si no hay salida a PyPI) |
| `validacion/validacion_entorno_lafise_ai_gateway.py` | Notebook de validación de entorno (para los organizadores, antes del taller) |
| `build/lafise-unity-ai-gateway.dbc` | El mismo árbol de notebooks en un solo archivo, para importar en un clic |

## Requisitos

- Clúster clásico **DBR 17.3 LTS**, modo de acceso **Standard** o **Dedicated** (Serverless también funciona).
- Unity Catalog y Foundation Model APIs (pay-per-token) habilitadas, en una región con Unity AI Gateway.
- En el catálogo del taller: `USE CATALOG` y `CREATE SCHEMA`. Cada participante crea su schema
  `ai_gateway_<usuario>` y dentro de él tablas y Model Services (`CREATE TABLE`, `CREATE SERVICE`).
- `EXECUTE` sobre los modelos `system.ai.*`. Opcional: `USE SCHEMA` + `SELECT` sobre `system.ai_gateway`.
- Salida a PyPI para `%pip install faker` (o instalar `faker>=25.0.0` como librería del clúster).

## Cómo cargarlo en Databricks

**Opción A (recomendada): archivo `.dbc`.** Importa todo el árbol de notebooks en un clic.
1. Descargar el archivo desde este repositorio: abrir `build/lafise-unity-ai-gateway.dbc` y pulsar **Download raw file**
   (o usar el enlace directo
   `https://github.com/FerHrndz7/lafise-taller-unity-ai-gateway/raw/main/build/lafise-unity-ai-gateway.dbc`).
2. En el workspace: **Workspace → Home → Import → File** y subir el `.dbc`.
3. Abrir `00 - Course Overview` y luego `Laboratorio - Unity AI Gateway/00_setup`.

**Opción B: Git folder.** No requiere descargar archivos.
1. En el workspace: **Workspace → Create → Git folder**.
2. URL: `https://github.com/FerHrndz7/lafise-taller-unity-ai-gateway`, proveedor **GitHub** (repositorio público, no
   requiere credenciales).

**Opción C: descargar el ZIP.** En GitHub: **Code → Download ZIP**, descomprimir e importar la carpeta
`lafise-unity-ai-gateway` al workspace.

## Cómo ejecutarlo

1. Conectar el **clúster clásico** a cada notebook.
2. La primera ejecución de cada notebook se **detiene a propósito**: escribir el catálogo del taller en el widget
   `catalog` (arriba del notebook) y volver a ejecutar.
3. Orden recomendado: `00_setup` → `01` → `02` → `04` → `03` → `05_reto`. La inference table tarda unos minutos en
   recibir los primeros registros; hacer el `04` antes del `03` da ese margen.

## Validación de entorno (organizadores)

Antes del taller, importar `validacion/validacion_entorno_lafise_ai_gateway.py`, adjuntar el clúster clásico, escribir
el catálogo en el widget y ejecutar **Run all** (~10 min), idealmente con un usuario sin permisos de administrador.
Prueba cada pieza que usa el laboratorio, imprime PASA / FALLA / ALTERNATIVA / REVISAR por requisito y borra todo lo
que creó.

## Limpieza

Al final, la sección D de `00 - Course Overview` trae una celda que borra los Model Services y el schema de cada
participante.
