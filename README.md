# Country Indicators Data Warehouse

Pipeline ETL que integra datos de **PostgreSQL** (relacional) y **MongoDB** (no relacional) en un **data warehouse** en PostgreSQL con modelo dimensional (estrella). Incluye indicadores por país: población, tasa de envejecimiento, índice Big Mac y costos turísticos.

---

## Nombre sugerido para el repositorio en GitHub

**`country-indicators-dw`**

- Describe el dominio (indicadores por país) y el propósito (data warehouse).
- Nomenclatura estándar en kebab-case para repositorios.
- Corto y fácil de buscar.

Alternativas: `geo-indicators-etl`, `country-analytics-dw`.

---

## Requisitos

- **Python 3.10+**
- **Docker y Docker Compose** (recomendado) o PostgreSQL 16+ y MongoDB 8.2+ en local

---

## Estructura del proyecto (listo para versionar en GitHub)

```
country-indicators-dw/
├── config/                      # Configuración
│   ├── config.yaml              # Parámetros ETL, esquemas, rutas
│   └── country_mapping.yaml     # Normalización de nombres de país
├── ddl/                         # DDL idempotente
│   ├── 01_source_schema.sql     # Tablas origen (PostgreSQL)
│   └── 02_warehouse_schema.sql  # Schema warehouse (dim_pais, hecho_indicadores)
├── etl/                         # Paquete ETL (extract, transform, load, quality)
├── scripts/                     # Puntos de entrada
│   ├── load_postgres_sources.py # Carga CSV → PostgreSQL
│   ├── load_mongo_sources.py    # Carga JSON → MongoDB
│   └── run_etl.py               # Ejecuta pipeline ETL completo
├── dags/                        # DAG Airflow (ETL programado)
│   └── country_indicators_dw_etl_dag.py
├── queries/                     # Consultas analíticas
│   └── insights.sql             # Tres insights sobre el DW
├── docs/                        # Documentación (arquitectura, diseño DW, diccionario, glosario)
│   ├── README.md                # Índice de la documentación
│   ├── 01_arquitectura.md
│   ├── 02_diseno_dw.md
│   ├── 03_diccionario_datos.md
│   └── 04_glosario.md
├── data/                        # Datos fuente (dentro del proyecto, sin rutas externas)
│   ├── sql/                     # pais_poblacion.csv, pais_envejecimiento.csv
│   └── mongo/                   # paises_mundo_big_mac.json, costos_turisticos_*.json
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Instrucciones de uso

### 1. Clonar o usar la carpeta como raíz del repo

Si clonas desde GitHub, la raíz del repo será `country-indicators-dw/`. Si estás dentro de otro directorio (p. ej. Laboratorio 05), entra en `country-indicators-dw` y trabaja desde ahí.

```bash
cd country-indicators-dw
```

### 2. Datos fuente (dentro del proyecto)

Todo el proyecto es **autocontenido**: los datos se leen solo desde **`data/sql/`** y **`data/mongo/`**, dentro de `country-indicators-dw/`. No se usa ninguna ruta fuera de este directorio.

- **SQL:** `data/sql/pais_poblacion.csv`, `data/sql/pais_envejecimiento.csv`
- **MongoDB:** `data/mongo/paises_mundo_big_mac.json`, `data/mongo/costos_turisticos_*.json`

Si clonó el repo sin datos, copie ahí los CSV y JSON correspondientes.

### 3. Levantar servicios con Docker

```bash
docker-compose up -d
```

Espere a que PostgreSQL y MongoDB estén en marcha. El DDL se aplica automáticamente al iniciar Postgres (carpeta `ddl/` montada en `docker-entrypoint-initdb.d`).

### 4. Entorno Python

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/macOS
pip install -r requirements.txt
cp .env.example .env            # Ajustar si usa otros host/puerto/usuario/contraseña
```

### 5. Cargar fuentes en las bases de datos

Desde la raíz del proyecto (`country-indicators-dw`), los scripts leen por defecto `data/sql` y `data/mongo`:

```bash
python scripts/load_postgres_sources.py
python scripts/load_mongo_sources.py
```

### 6. Ejecutar el ETL (integrar y cargar al data warehouse)

```bash
python scripts/run_etl.py
```

### 7. Ejecutar consultas de insights

Si tiene `psql` instalado en el host (puerto 5433):

```bash
psql -h localhost -p 5433 -U ci_dw -d country_indicators_dw -f queries/insights.sql
```

Si solo usa Docker y no tiene `psql` instalado, ejecute dentro del contenedor:

```bash
# Bash / Git Bash (Windows)
cat queries/insights.sql | docker exec -i country-indicators-dw-postgres psql -U ci_dw -d country_indicators_dw

# PowerShell (Windows)
Get-Content queries/insights.sql -Raw | docker exec -i country-indicators-dw-postgres psql -U ci_dw -d country_indicators_dw
```

(Contraseña: `change_me_ci_dw`. No se pide al usar `docker exec` porque la sesión es local al contenedor.)

---

## ETL programado (Airflow)

Por defecto, el DAG de Airflow ejecuta el pipeline cada 10 segundos (intervalo definido en `dags/country_indicators_dw_etl_dag.py`, parámetro `schedule`). Puede ajustar ese valor si necesita otra frecuencia.

### Requisitos

- **Python 3.10, 3.11, 3.12 o 3.13** en el entorno donde instale Airflow (recomendado 3.12 para coincidir con los constraints publicados).
- Haber seguido los pasos anteriores de este README:
  - Levantar los contenedores con:
    ```bash
    docker-compose up -d
    ```
  - Crear y activar el entorno virtual del proyecto y instalar dependencias:
    ```bash
    python -m venv .venv
    .venv\Scripts\activate          # Windows
    # source .venv/bin/activate     # Linux/macOS
    pip install -r requirements.txt
    ```
  - Copiar `.env.example` a `.env` y ajustar si es necesario.

### Pasos

1. **Instalar Airflow en un entorno separado** (recomendado fuera de este proyecto):
   ```bash
   python -m venv airflow-venv
   airflow-venv\Scripts\activate      # Windows (CMD/PowerShell)
   # source airflow-venv/Scripts/activate  # Git Bash (Windows)
   # source airflow-venv/bin/activate      # Linux/macOS

   export AIRFLOW_VERSION=3.1.7
   export PYTHON_VERSION=3.12
   export CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
   pip install "apache-airflow==${AIRFLOW_VERSION}" "apache-airflow-providers-standard" --constraint "${CONSTRAINT_URL}"
   ```

   **Si en Windows falla la instalación (grpcio / Visual C++):** Si aparece un error tipo `Microsoft Visual C++ 14.0 or greater is required` o `Failed building wheel for grpcio`, es porque la dependencia `grpcio` intenta compilarse y en Windows hace falta un compilador C++. Opciones:
   - **Instalar Microsoft C++ Build Tools** (varios GB): descargue [Build Tools para Visual Studio](https://visualstudio.microsoft.com/visual-cpp-build-tools/) e instale la carga de trabajo "Desarrollo para el escritorio con C++". Luego repita el `pip install` del paso 1.
   - **Usar un entorno Linux:** ejecute Airflow dentro de **WSL2** o en un contenedor **Docker** con imagen Linux; allí pip suele usar ruedas precompiladas de `grpcio` y no se necesita Visual C++.

2. **Definir AIRFLOW_HOME** (carpeta donde Airflow guarda config y DAGs):
   ```bash
   export AIRFLOW_HOME=~/airflow   # Linux/macOS
   # En Windows (PowerShell): $env:AIRFLOW_HOME = "C:\airflow"
   ```

3. **Inicializar la base de datos de Airflow** (solo la primera vez):
   ```bash
   airflow db init
   airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@localhost
   ```

4. **Copiar el DAG** al directorio que Airflow escanea:
   ```bash
   mkdir -p "$AIRFLOW_HOME/dags"
   cp dags/country_indicators_dw_etl_dag.py "$AIRFLOW_HOME/dags/"
   ```

5. **Cargar variables de entorno del proyecto** antes de arrancar Airflow (para que conecte a Postgres y Mongo). Desde la raíz del proyecto:
   ```bash
   set -a && source .env && set +a   # Bash (Linux/macOS/Git Bash)
   # En Windows PowerShell: Get-Content .env | ForEach-Object { if ($_ -match '^([^#][^=]+)=(.*)$') { [Environment]::SetEnvironmentVariable($matches[1], $matches[2], 'Process') } }
   ```

6. **Iniciar Airflow 3** (api-server, dag-processor y scheduler; en tres terminales o en segundo plano):
   - Terminal 1 (API server, sustituye al webserver de Airflow 2):
     ```bash
     airflow api-server
     ```
   - Terminal 2 (procesador de DAGs, obligatorio en Airflow 3):
     ```bash
     airflow dag-processor
     ```
   - Terminal 3:
     ```bash
     airflow scheduler
     ```
   Abra http://localhost:8080 (usuario `admin`, contraseña la que creó en el paso 2). El DAG `country_indicators_dw_etl` debe aparecer y ejecutarse de acuerdo con el intervalo configurado (por defecto 10 segundos).

### Alternativa sin Airflow

Programar a mano: ejecutar `python scripts/run_etl.py` con **cron** / **Programador de tareas** (Windows) a la frecuencia que quiera (por ejemplo cada 10 segundos para pruebas), por ejemplo:
```bash
python scripts/run_etl.py
```

---

## Variables de entorno

| Variable | Descripción | Valor por defecto |
|----------|-------------|--------------------|
| POSTGRES_HOST | Host PostgreSQL | localhost |
| POSTGRES_PORT | Puerto PostgreSQL | 5433 (Docker publica en 5433 para evitar conflicto con 5432) |
| POSTGRES_USER | Usuario PostgreSQL | ci_dw |
| POSTGRES_PASSWORD | Contraseña PostgreSQL | change_me_ci_dw |
| POSTGRES_DB | Base de datos | country_indicators_dw |
| MONGO_HOST | Host MongoDB | localhost |
| MONGO_PORT | Puerto MongoDB | 27017 |
| MONGO_DB | Base de datos MongoDB | country_indicators_dw |

Copie `.env.example` a `.env` y ajuste según su entorno. No suba `.env` al repositorio.

---

## Nomenclatura estándar usada en el proyecto

| Concepto | Nombre en el proyecto |
|----------|------------------------|
| Proyecto / repositorio | country-indicators-dw |
| Base de datos (PostgreSQL y MongoDB) | country_indicators_dw |
| Usuario PostgreSQL | ci_dw (country indicators data warehouse) |
| Schema del data warehouse | warehouse |
| Dimensión país | dim_pais |
| Tabla de hechos de indicadores | hecho_indicadores |
| DAG Airflow | country_indicators_dw_etl |
| Contenedores Docker | country-indicators-dw-postgres, country-indicators-dw-mongo |

---

## Entregables típicos (laboratorio / curso)

- **PDF:** Evidencia de carga en PostgreSQL y MongoDB, ejecución de ETL y los tres insights.
- **Código fuente:** Este repositorio (carpeta `country-indicators-dw` o el repo `country-indicators-dw` en GitHub).
- **Script de queries:** `queries/insights.sql`.

---

## Referencias

- Documentación: carpeta [docs/](docs/README.md) (arquitectura, diseño del DW, diccionario de datos, glosario).
- Modelo del DW: schema `warehouse`, tablas `dim_pais` y `hecho_indicadores`, vistas `v_pais_completo` y `v_por_continente`.
