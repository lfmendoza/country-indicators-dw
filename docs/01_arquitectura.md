# Arquitectura del sistema

## Visión general

El proyecto integra datos de dos fuentes (PostgreSQL y MongoDB) en un data warehouse en PostgreSQL mediante un pipeline ETL en Python. Todo se ejecuta dentro del directorio del proyecto; los datos de entrada están en `data/sql` y `data/mongo`.

## Componentes

```
┌────────────────────────────────────────────────────────────────────────┐
│                         country-indicators-dw                          │
├────────────────────────────────────────────────────────────────────────┤
│  data/sql           data/mongo         config/        ddl/             │
│  (CSV)               (JSON)            (YAML)         (SQL init)       │
│      │                    │                │                │          │
│      ▼                    ▼                ▼                ▼          │
│  scripts/load_postgres_sources.py   scripts/load_mongo_sources.py      │
│      │                    │                                            │
│      ▼                    ▼                                            │
│  ┌──────────────┐   ┌──────────────┐                                   │
│  │  PostgreSQL  │   │   MongoDB    │   ← Fuentes (staging)             │
│  │  (public)    │   │  (lab DB)    │                                   │
│  └──────┬───────┘   └──────┬───────┘                                   │
│         │                  │                                           │
│         └─────────┬────────┘                                           │
│                   ▼                                                    │
│            etl/ (extract → transform → load)                           │
│                   │                                                    │
│                   ▼                                                    │
│  ┌──────────────────────────────────┐                                  │
│  │  PostgreSQL – schema warehouse   │   ← Data warehouse               │
│  │  (dim_pais, hecho_indicadores)   │                                  │
│  └──────────────────────────────────┘                                  │
│                   │                                                    │
│  scripts/run_etl.py   dags/country_indicators_dw_etl_dag.py (Airflow)  │
│  queries/insights.sql                                                  │
└────────────────────────────────────────────────────────────────────────┘
```

## Capas

| Capa | Ubicación | Responsabilidad |
|------|-----------|-----------------|
| Fuentes | `data/sql`, `data/mongo` | Archivos CSV y JSON de entrada |
| Carga inicial | `scripts/load_*_sources.py` | Poblado de PostgreSQL y MongoDB desde archivos |
| Origen | PostgreSQL (public), MongoDB | Tablas/colecciones de staging |
| ETL | `etl/` (extract, transform, quality, load) | Extracción, normalización, unión por país, carga al warehouse |
| Warehouse | PostgreSQL schema `warehouse` | Modelo dimensional (dim_pais, hecho_indicadores) |
| Consumo | `queries/insights.sql`, vistas | Consultas analíticas e insights |

## Flujo de datos

1. **Carga de fuentes:** Los scripts leen CSV desde `data/sql` y JSON desde `data/mongo` y cargan en PostgreSQL (tablas `pais_poblacion`, `pais_envejecimiento`) y MongoDB (colecciones `paises_big_mac`, `costos_turisticos`).
2. **ETL:** El pipeline extrae de ambas bases, normaliza nombres de país (`config/country_mapping.yaml`), une por país, aplica reglas de calidad y escribe en `warehouse.dim_pais` (upsert) y `warehouse.hecho_indicadores` (full refresh).
3. **Análisis:** Las consultas de `queries/insights.sql` y las vistas `v_pais_completo` y `v_por_continente` leen del warehouse.

## Infraestructura

- **PostgreSQL:** Una instancia; schema `public` para staging, schema `warehouse` para el DW.
- **MongoDB:** Una base; colecciones para Big Mac y costos turísticos.
- **Ejecución:** Manual (`python scripts/run_etl.py`) o programada (DAG de Airflow o cron).

## Dependencias externas

- Python 3.10+, psycopg2, pymongo, pandas, PyYAML, python-dotenv.
- Acceso a PostgreSQL y MongoDB (locales o en Docker según `docker-compose.yml`).
