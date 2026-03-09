# Glosario

| Término | Definición |
|---------|------------|
| dim_pais | Dimensión del warehouse: una fila por país con nombre normalizado, continente, región, capital. |
| hecho_indicadores | Tabla de hechos: métricas por país (población, envejecimiento, Big Mac, costos) por ejecución del ETL. |
| nombre_pais_normalizado | Nombre canónico del país usado como clave de join entre PostgreSQL y MongoDB; definido en `config/country_mapping.yaml`. |
| full refresh | Estrategia de carga del hecho: en cada ejecución se trunca `hecho_indicadores` y se insertan de nuevo todos los registros. |
| warehouse | Schema de PostgreSQL donde están `dim_pais`, `hecho_indicadores` y las vistas del DW. |
| ETL | Extract (desde PostgreSQL y MongoDB), Transform (normalización, merge, calidad), Load (upsert dim_pais, insert hecho_indicadores). |
