# Diseño del Data Warehouse

## Modelo dimensional (estrella)

El warehouse usa un esquema en estrella en el schema `warehouse` de PostgreSQL:

- **Una dimensión:** `dim_pais` (contexto por país).
- **Una tabla de hechos:** `hecho_indicadores` (métricas por país y carga).

La clave de integración entre fuentes es el nombre de país normalizado (`nombre_pais_normalizado`).

## Dimensión: dim_pais

| Atributo | Tipo | Descripción |
|----------|------|-------------|
| id_pais | SERIAL | Clave sustituta (PK). |
| nombre_pais_normalizado | VARCHAR(100) | Nombre canónico del país (único). Clave natural para el ETL. |
| continente | VARCHAR(80) | Continente. |
| region | VARCHAR(100) | Subregión. |
| capital | VARCHAR(100) | Capital. |

Una fila por país. El ETL hace upsert por `nombre_pais_normalizado`.

## Tabla de hechos: hecho_indicadores

| Atributo | Tipo | Descripción |
|----------|------|-------------|
| id_hecho | BIGSERIAL | PK. |
| id_pais | INTEGER | FK a dim_pais. |
| poblacion | BIGINT | Población. |
| tasa_envejecimiento | NUMERIC(6,2) | Tasa de envejecimiento (%). |
| precio_big_mac_usd | NUMERIC(8,2) | Precio Big Mac (USD). |
| costo_hospedaje_prom_usd | NUMERIC(8,2) | Costo promedio hospedaje (USD). |
| costo_comida_prom_usd | NUMERIC(8,2) | Costo promedio comida (USD). |
| costo_transporte_prom_usd | NUMERIC(8,2) | Costo promedio transporte (USD). |
| costo_entretenimiento_prom_usd | NUMERIC(8,2) | Costo promedio entretenimiento (USD). |
| costo_diario_total_prom_usd | NUMERIC(10,2) | Suma de los cuatro costos promedio (USD). |
| fecha_carga | TIMESTAMPTZ | Momento de la carga. |
| batch_id | VARCHAR(64) | Identificador opcional de la ejecución. |

Grano: una fila por país por ejecución del ETL. Estrategia de carga: full refresh (truncate + insert) en cada ejecución.

## Vistas

- **v_pais_completo:** Una fila por país con la última versión de sus indicadores (LATERAL join a la última `fecha_carga` por `id_pais`).
- **v_por_continente:** Agregados por continente (conteo de países, promedios de población, tasa de envejecimiento, precio Big Mac, costo diario total) sobre la última carga.

## Reglas de integración

- **Población:** Se prioriza el valor de MongoDB (costos_turisticos); si falta, se usa el de PostgreSQL (pais_poblacion).
- **Costos (hospedaje, comida, transporte, entretenimiento):** Se prioriza el promedio calculado desde MongoDB; si falta, se usan los campos de bajo/promedio de PostgreSQL.
- **País:** Se aplica el mapeo de `config/country_mapping.yaml` para obtener `nombre_pais_normalizado` y hacer el join entre fuentes.
