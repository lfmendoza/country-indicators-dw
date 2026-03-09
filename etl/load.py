from __future__ import annotations

import logging
import os
from typing import Any

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

from etl.config import get_config

logger = logging.getLogger(__name__)


def _pg_connection():
    cfg = get_config()["postgres"]
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg["password"],
        dbname=cfg["dbname"],
    )


def load_dim_pais(conn, df: pd.DataFrame, warehouse_schema: str = "warehouse") -> dict[str, int]:
    dim = df[["nombre_pais_normalizado", "continente", "region", "capital"]].drop_duplicates("nombre_pais_normalizado")
    dim = dim[dim["nombre_pais_normalizado"].notna() & (dim["nombre_pais_normalizado"].astype(str).str.strip() != "")]
    with conn.cursor() as cur:
        for _, row in dim.iterrows():
            cur.execute(
                f"""
                INSERT INTO {warehouse_schema}.dim_pais (nombre_pais_normalizado, continente, region, capital)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (nombre_pais_normalizado) DO UPDATE SET
                    continente = COALESCE(EXCLUDED.continente, {warehouse_schema}.dim_pais.continente),
                    region = COALESCE(EXCLUDED.region, {warehouse_schema}.dim_pais.region),
                    capital = COALESCE(EXCLUDED.capital, {warehouse_schema}.dim_pais.capital),
                    updated_at = CURRENT_TIMESTAMP
                RETURNING id_pais
                """,
                (row["nombre_pais_normalizado"], row.get("continente"), row.get("region"), row.get("capital")),
            )
        cur.execute(f"SELECT id_pais, nombre_pais_normalizado FROM {warehouse_schema}.dim_pais")
        mapping = {str(r[1]): r[0] for r in cur.fetchall()}
    logger.info("dim_pais: %s rows upserted, %s total in dimension", len(dim), len(mapping))
    return mapping


def load_hecho_indicadores(
    conn,
    df: pd.DataFrame,
    id_pais_mapping: dict[str, int],
    warehouse_schema: str = "warehouse",
    full_refresh: bool = True,
    batch_id: str | None = None,
) -> int:
    batch_id = batch_id or os.environ.get("ETL_BATCH_ID") or None
    df = df.copy()
    df["id_pais"] = df["nombre_pais_normalizado"].astype(str).map(id_pais_mapping)
    df = df[df["id_pais"].notna()]
    df["id_pais"] = df["id_pais"].astype(int)

    with conn.cursor() as cur:
        if full_refresh:
            cur.execute(f"TRUNCATE TABLE {warehouse_schema}.hecho_indicadores RESTART IDENTITY CASCADE")
            logger.info("hecho_indicadores truncated (full refresh)")

        rows = []
        for _, row in df.iterrows():
            rows.append((
                int(row["id_pais"]),
                int(row["poblacion"]) if pd.notna(row.get("poblacion")) and str(row.get("poblacion")) != "nan" else None,
                float(row["tasa_envejecimiento"]) if pd.notna(row.get("tasa_envejecimiento")) else None,
                float(row["precio_big_mac_usd"]) if pd.notna(row.get("precio_big_mac_usd")) else None,
                float(row["costo_hospedaje_prom_usd"]) if pd.notna(row.get("costo_hospedaje_prom_usd")) else None,
                float(row["costo_comida_prom_usd"]) if pd.notna(row.get("costo_comida_prom_usd")) else None,
                float(row["costo_transporte_prom_usd"]) if pd.notna(row.get("costo_transporte_prom_usd")) else None,
                float(row["costo_entretenimiento_prom_usd"]) if pd.notna(row.get("costo_entretenimiento_prom_usd")) else None,
                float(row["costo_diario_total_prom_usd"]) if pd.notna(row.get("costo_diario_total_prom_usd")) else None,
                batch_id,
            ))
        if rows:
            execute_values(
                cur,
                f"""
                INSERT INTO {warehouse_schema}.hecho_indicadores (
                    id_pais, poblacion, tasa_envejecimiento, precio_big_mac_usd,
                    costo_hospedaje_prom_usd, costo_comida_prom_usd, costo_transporte_prom_usd, costo_entretenimiento_prom_usd,
                    costo_diario_total_prom_usd, batch_id
                ) VALUES %s
                """,
                rows,
                page_size=500,
            )
    count = len(rows)
    logger.info("hecho_indicadores: %s rows loaded", count)
    return count


def load_warehouse(df: pd.DataFrame) -> dict[str, Any]:
    cfg = get_config()
    schema = cfg.get("postgres", {}).get("warehouse_schema", "warehouse")
    full_refresh = cfg.get("etl", {}).get("full_refresh_hecho", True)

    conn = _pg_connection()
    try:
        conn.autocommit = False
        id_map = load_dim_pais(conn, df, warehouse_schema=schema)
        n = load_hecho_indicadores(conn, df, id_map, warehouse_schema=schema, full_refresh=full_refresh)
        conn.commit()
        return {"dim_pais_count": len(id_map), "hecho_indicadores_count": n}
    except Exception as e:
        conn.rollback()
        logger.exception("Load failed: %s", e)
        raise
    finally:
        conn.close()
