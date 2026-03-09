# Diccionario de datos

## Fuentes (staging)

### pais_poblacion (PostgreSQL, public)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| _id | VARCHAR(24) | Identificador (origen CSV) |
| continente | VARCHAR(80) | Continente |
| pais | VARCHAR(100) | Nombre del país |
| poblacion | BIGINT | Población |
| costo_bajo_hospedaje | NUMERIC(10,2) | Costo bajo hospedaje (USD) |
| costo_promedio_comida | NUMERIC(10,2) | Costo promedio comida (USD) |
| costo_bajo_transporte | NUMERIC(10,2) | Costo bajo transporte (USD) |
| costo_promedio_entretenimiento | NUMERIC(10,2) | Costo promedio entretenimiento (USD) |

### pais_envejecimiento (PostgreSQL, public)

| Columna | Tipo | Descripción |
|---------|------|-------------|
| id_pais | INTEGER | PK |
| nombre_pais | VARCHAR(100) | Nombre del país |
| capital | VARCHAR(100) | Capital |
| continente | VARCHAR(80) | Continente |
| region | VARCHAR(100) | Región |
| poblacion | NUMERIC(18,2) | Población |
| tasa_de_envejecimiento | NUMERIC(6,2) | Tasa de envejecimiento (%) |

### paises_big_mac (MongoDB)

| Campo | Descripción |
|-------|-------------|
| pais | Nombre del país |
| continente | Continente |
| precio_big_mac_usd | Precio Big Mac (USD) |

### costos_turisticos (MongoDB)

| Campo | Descripción |
|-------|-------------|
| pais | Nombre del país |
| continente | Continente |
| region | Región |
| capital | Capital |
| poblacion | Población |
| costos_diarios_estimados_en_dolares | Objeto con hospedaje, comida, transporte, entretenimiento. Cada uno con precio_bajo_usd, precio_promedio_usd, precio_alto_usd. |

---

## Data Warehouse (schema warehouse)

### dim_pais

| Columna | Tipo | Descripción |
|---------|------|-------------|
| id_pais | SERIAL | PK, clave sustituta |
| nombre_pais_normalizado | VARCHAR(100) | Nombre canónico para join (único) |
| continente | VARCHAR(80) | Continente |
| region | VARCHAR(100) | Región |
| capital | VARCHAR(100) | Capital |

### hecho_indicadores

| Columna | Tipo | Descripción |
|---------|------|-------------|
| id_hecho | BIGSERIAL | PK |
| id_pais | INTEGER | FK a dim_pais |
| poblacion | BIGINT | Población |
| tasa_envejecimiento | NUMERIC(6,2) | Tasa de envejecimiento (%) |
| precio_big_mac_usd | NUMERIC(8,2) | Precio Big Mac (USD) |
| costo_hospedaje_prom_usd | NUMERIC(8,2) | Costo promedio hospedaje (USD) |
| costo_comida_prom_usd | NUMERIC(8,2) | Costo promedio comida (USD) |
| costo_transporte_prom_usd | NUMERIC(8,2) | Costo promedio transporte (USD) |
| costo_entretenimiento_prom_usd | NUMERIC(8,2) | Costo promedio entretenimiento (USD) |
| costo_diario_total_prom_usd | NUMERIC(10,2) | Suma de los cuatro costos promedio (USD) |
| fecha_carga | TIMESTAMPTZ | Momento de carga |
| batch_id | VARCHAR(64) | Identificador de ejecución (opcional) |

### Vistas

| Vista | Descripción |
|-------|-------------|
| v_pais_completo | Una fila por país con el último registro de hecho_indicadores. |
| v_por_continente | Agregados por continente (número de países, promedios de población, tasa de envejecimiento, precio Big Mac, costo diario total). |
