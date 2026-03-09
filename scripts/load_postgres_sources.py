#!/usr/bin/env python3
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_connection():
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5433"))
    user = os.environ.get("POSTGRES_USER", "ci_dw")
    password = os.environ.get("POSTGRES_PASSWORD", "change_me_ci_dw")
    dbname = os.environ.get("POSTGRES_DB", "country_indicators_dw")
    return psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=dbname
    )


def load_pais_poblacion(conn, data_dir: Path) -> int:
    path = data_dir / "pais_poblacion.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    df = pd.read_csv(path)
    df = df.astype(object).where(pd.notnull(df), None)
    cols = [
        "_id", "continente", "pais", "poblacion",
        "costo_bajo_hospedaje", "costo_promedio_comida",
        "costo_bajo_transporte", "costo_promedio_entretenimiento",
    ]
    df = df[[c for c in cols if c in df.columns]]
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE pais_poblacion CASCADE")
        execute_values(
            cur,
            """
            INSERT INTO pais_poblacion (_id, continente, pais, poblacion,
                costo_bajo_hospedaje, costo_promedio_comida,
                costo_bajo_transporte, costo_promedio_entretenimiento)
            VALUES %s
            """,
            [tuple(row) for row in df.itertuples(index=False, name=None)],
            page_size=500,
        )
    logger.info("Loaded %s rows into pais_poblacion", len(df))
    return len(df)


def load_pais_envejecimiento(conn, data_dir: Path) -> int:
    path = data_dir / "pais_envejecimiento.csv"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    df = pd.read_csv(path)
    df = df.astype(object).where(pd.notnull(df), None)
    cols = [
        "id_pais", "nombre_pais", "capital", "continente", "region",
        "poblacion", "tasa_de_envejecimiento",
    ]
    df = df[[c for c in cols if c in df.columns]]
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE pais_envejecimiento CASCADE")
        execute_values(
            cur,
            """
            INSERT INTO pais_envejecimiento (id_pais, nombre_pais, capital, continente, region, poblacion, tasa_de_envejecimiento)
            VALUES %s
            """,
            [tuple(row) for row in df.itertuples(index=False, name=None)],
            page_size=500,
        )
    logger.info("Loaded %s rows into pais_envejecimiento", len(df))
    return len(df)


def main():
    parser = argparse.ArgumentParser(description="Load CSV sources into PostgreSQL")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "sql",
        help="Directory containing pais_poblacion.csv and pais_envejecimiento.csv",
    )
    args = parser.parse_args()
    data_dir = args.data_dir.resolve()
    if not data_dir.is_dir():
        logger.error("Data dir not found: %s", data_dir)
        sys.exit(1)

    conn = None
    try:
        conn = get_connection()
        conn.autocommit = False
        load_pais_poblacion(conn, data_dir)
        load_pais_envejecimiento(conn, data_dir)
        conn.commit()
        logger.info("PostgreSQL source load completed successfully.")
    except Exception as e:
        logger.exception("Load failed: %s", e)
        if conn:
            conn.rollback()
        sys.exit(1)
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    main()
