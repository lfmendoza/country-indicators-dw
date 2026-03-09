CREATE SCHEMA IF NOT EXISTS warehouse;

CREATE TABLE IF NOT EXISTS warehouse.dim_pais (
    id_pais                    SERIAL PRIMARY KEY,
    nombre_pais_normalizado    VARCHAR(100) NOT NULL,
    continente                 VARCHAR(80),
    region                     VARCHAR(100),
    capital                    VARCHAR(100),
    created_at                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at                 TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_dim_pais_nombre UNIQUE (nombre_pais_normalizado)
);

CREATE INDEX IF NOT EXISTS ix_dim_pais_continente ON warehouse.dim_pais (continente);
CREATE INDEX IF NOT EXISTS ix_dim_pais_region ON warehouse.dim_pais (region);

CREATE TABLE IF NOT EXISTS warehouse.hecho_indicadores (
    id_hecho                   BIGSERIAL PRIMARY KEY,
    id_pais                    INTEGER NOT NULL REFERENCES warehouse.dim_pais(id_pais),
    poblacion                  BIGINT,
    tasa_envejecimiento        NUMERIC(6,2),
    precio_big_mac_usd         NUMERIC(8,2),
    costo_hospedaje_prom_usd   NUMERIC(8,2),
    costo_comida_prom_usd      NUMERIC(8,2),
    costo_transporte_prom_usd  NUMERIC(8,2),
    costo_entretenimiento_prom_usd NUMERIC(8,2),
    costo_diario_total_prom_usd NUMERIC(10,2),
    fecha_carga                TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id                   VARCHAR(64),
    CONSTRAINT fk_hecho_dim_pais FOREIGN KEY (id_pais) REFERENCES warehouse.dim_pais(id_pais)
);

CREATE INDEX IF NOT EXISTS ix_hecho_indicadores_id_pais ON warehouse.hecho_indicadores (id_pais);
CREATE INDEX IF NOT EXISTS ix_hecho_indicadores_fecha_carga ON warehouse.hecho_indicadores (fecha_carga);

CREATE OR REPLACE VIEW warehouse.v_pais_completo AS
SELECT
    d.id_pais,
    d.nombre_pais_normalizado,
    d.continente,
    d.region,
    d.capital,
    h.poblacion,
    h.tasa_envejecimiento,
    h.precio_big_mac_usd,
    h.costo_hospedaje_prom_usd,
    h.costo_comida_prom_usd,
    h.costo_transporte_prom_usd,
    h.costo_entretenimiento_prom_usd,
    h.costo_diario_total_prom_usd,
    h.fecha_carga
FROM warehouse.dim_pais d
LEFT JOIN LATERAL (
    SELECT *
    FROM warehouse.hecho_indicadores h2
    WHERE h2.id_pais = d.id_pais
    ORDER BY h2.fecha_carga DESC
    LIMIT 1
) h ON true;

CREATE OR REPLACE VIEW warehouse.v_por_continente AS
SELECT
    d.continente,
    COUNT(*) AS num_paises,
    ROUND(AVG(h.poblacion), 0)::BIGINT AS poblacion_promedio,
    ROUND(AVG(h.tasa_envejecimiento), 2) AS tasa_envejecimiento_prom,
    ROUND(AVG(h.precio_big_mac_usd), 2) AS precio_big_mac_prom_usd,
    ROUND(AVG(h.costo_diario_total_prom_usd), 2) AS costo_diario_total_prom_usd
FROM warehouse.dim_pais d
JOIN warehouse.hecho_indicadores h ON d.id_pais = h.id_pais
WHERE h.fecha_carga = (SELECT MAX(fecha_carga) FROM warehouse.hecho_indicadores)
GROUP BY d.continente
ORDER BY costo_diario_total_prom_usd DESC NULLS LAST;
