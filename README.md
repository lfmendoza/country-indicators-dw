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
- **Docker y Docker Compose** (recomendado) o PostgreSQL 15+ y MongoDB 7+ en local

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

```bash
psql -h localhost -U ci_dw -d country_indicators_dw -f queries/insights.sql
```

(Contraseña por defecto en Docker: `change_me_ci_dw`.)

---

## ETL programado (Airflow)

Para ejecutar el mismo pipeline en un horario (p. ej. diario):

- Copie `dags/country_indicators_dw_etl_dag.py` en su `AIRFLOW_HOME/dags/`.
- Asegure que el entorno de Airflow tenga acceso a PostgreSQL y MongoDB (mismas variables de entorno o `.env`).

Alternativa sin Airflow: programar `python scripts/run_etl.py` con **cron** (Linux/macOS) o **Programador de tareas** (Windows).

---

## Variables de entorno

| Variable | Descripción | Valor por defecto |
|----------|-------------|--------------------|
| POSTGRES_HOST | Host PostgreSQL | localhost |
| POSTGRES_PORT | Puerto PostgreSQL | 5432 |
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
