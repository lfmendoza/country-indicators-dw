from __future__ import annotations

import logging
from typing import Any

import pandas as pd
import psycopg2
from pymongo import MongoClient

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


def _mongo_client():
    cfg = get_config()["mongo"]
    uri = f"mongodb://{cfg['host']}:{cfg['port']}/"
    if cfg.get("user") and cfg.get("password"):
        uri = f"mongodb://{cfg['user']}:{cfg['password']}@{cfg['host']}:{cfg['port']}/"
    return MongoClient(uri)


def extract_pais_poblacion() -> pd.DataFrame:
    conn = _pg_connection()
    try:
        df = pd.read_sql(
            "SELECT _id, continente, pais, poblacion, costo_bajo_hospedaje, costo_promedio_comida, costo_bajo_transporte, costo_promedio_entretenimiento FROM pais_poblacion",
            conn,
        )
        logger.info("Extracted pais_poblacion: %s rows", len(df))
        return df
    finally:
        conn.close()


def extract_pais_envejecimiento() -> pd.DataFrame:
    conn = _pg_connection()
    try:
        df = pd.read_sql(
            "SELECT id_pais, nombre_pais, capital, continente, region, poblacion, tasa_de_envejecimiento FROM pais_envejecimiento",
            conn,
        )
        logger.info("Extracted pais_envejecimiento: %s rows", len(df))
        return df
    finally:
        conn.close()


def extract_big_mac() -> pd.DataFrame:
    cfg = get_config()
    client = _mongo_client()
    try:
        db = client[cfg["mongo"]["dbname"]]
        coll_name = cfg.get("mongo", {}).get("collection_big_mac") or "paises_big_mac"
        coll = db[coll_name]
        cursor = coll.find({}, {"_id": 0, "pais": 1, "continente": 1, "precio_big_mac_usd": 1})
        docs = list(cursor)
        df = pd.DataFrame(docs)
        logger.info("Extracted Big Mac: %s rows", len(df))
        return df
    finally:
        client.close()


def extract_costos_turisticos() -> pd.DataFrame:
    cfg = get_config()
    client = _mongo_client()
    try:
        db = client[cfg["mongo"]["dbname"]]
        coll_name = cfg.get("mongo", {}).get("collection_costos") or "costos_turisticos"
        coll = db[coll_name]
        cursor = coll.find()
        rows = []
        for doc in cursor:
            costos = doc.get("costos_diarios_estimados_en_dolares") or doc.get("costos_diarios_estimados_en_dólares") or {}
            def avg(d: dict[str, Any]) -> float | None:
                if not d: return None
                lo = d.get("precio_bajo_usd")
                mid = d.get("precio_promedio_usd")
                hi = d.get("precio_alto_usd")
                vals = [x for x in (lo, mid, hi) if x is not None]
                return sum(vals) / len(vals) if vals else None
            rows.append({
                "pais": doc.get("pais"),
                "continente": doc.get("continente"),
                "region": doc.get("region"),
                "capital": doc.get("capital"),
                "poblacion": doc.get("poblacion"),
                "costo_hospedaje_prom_usd": avg(costos.get("hospedaje") or {}),
                "costo_comida_prom_usd": avg(costos.get("comida") or {}),
                "costo_transporte_prom_usd": avg(costos.get("transporte") or {}),
                "costo_entretenimiento_prom_usd": avg(costos.get("entretenimiento") or {}),
            })
        df = pd.DataFrame(rows)
        logger.info("Extracted costos_turisticos: %s rows", len(df))
        return df
    finally:
        client.close()


def extract_all() -> dict[str, pd.DataFrame]:
    return {
        "pais_poblacion": extract_pais_poblacion(),
        "pais_envejecimiento": extract_pais_envejecimiento(),
        "big_mac": extract_big_mac(),
        "costos_turisticos": extract_costos_turisticos(),
    }
