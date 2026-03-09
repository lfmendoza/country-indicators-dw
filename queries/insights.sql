SELECT
    d.nombre_pais_normalizado,
    d.continente,
    h.tasa_envejecimiento,
    h.precio_big_mac_usd,
    h.costo_diario_total_prom_usd
FROM warehouse.dim_pais d
JOIN warehouse.hecho_indicadores h ON d.id_pais = h.id_pais
WHERE h.tasa_envejecimiento IS NOT NULL
  AND h.precio_big_mac_usd IS NOT NULL
  AND h.fecha_carga = (SELECT MAX(fecha_carga) FROM warehouse.hecho_indicadores)
ORDER BY h.tasa_envejecimiento DESC;

SELECT
    d.nombre_pais_normalizado,
    d.continente,
    d.region,
    h.costo_diario_total_prom_usd,
    h.precio_big_mac_usd,
    ROUND(h.costo_diario_total_prom_usd / NULLIF(h.precio_big_mac_usd, 0), 1) AS ratio_dias_big_mac
FROM warehouse.dim_pais d
JOIN warehouse.hecho_indicadores h ON d.id_pais = h.id_pais
WHERE h.costo_diario_total_prom_usd IS NOT NULL
  AND h.fecha_carga = (SELECT MAX(fecha_carga) FROM warehouse.hecho_indicadores)
ORDER BY h.costo_diario_total_prom_usd ASC
LIMIT 20;

SELECT
    d.continente,
    COUNT(*) AS num_paises,
    ROUND(AVG(h.costo_diario_total_prom_usd), 2) AS costo_promedio_usd,
    ROUND(AVG(h.tasa_envejecimiento), 2) AS tasa_envejecimiento_prom
FROM warehouse.dim_pais d
JOIN warehouse.hecho_indicadores h ON d.id_pais = h.id_pais
WHERE h.fecha_carga = (SELECT MAX(fecha_carga) FROM warehouse.hecho_indicadores)
GROUP BY d.continente
ORDER BY costo_promedio_usd DESC NULLS LAST;
