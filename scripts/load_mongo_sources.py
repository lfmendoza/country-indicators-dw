#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from pymongo import MongoClient
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def get_client():
    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    user = os.environ.get("MONGO_USER") or None
    password = os.environ.get("MONGO_PASSWORD") or None
    dbname = os.environ.get("MONGO_DB", "country_indicators_dw")
    uri = f"mongodb://{host}:{port}/"
    if user and password:
        uri = f"mongodb://{user}:{password}@{host}:{port}/"
    client = MongoClient(uri)
    return client, dbname


def load_big_mac(client, dbname: str, data_dir: Path) -> int:
    path = data_dir / "paises_mundo_big_mac.json"
    if not path.exists():
        raise FileNotFoundError(f"Missing {path}")
    with open(path, "r", encoding="utf-8") as f:
        docs = json.load(f)
    if not isinstance(docs, list):
        docs = [docs]
    normalized = []
    for d in docs:
        normalized.append({
            "pais": d.get("país", d.get("pais", "")),
            "continente": d.get("continente"),
            "precio_big_mac_usd": float(d["precio_big_mac_usd"]) if d.get("precio_big_mac_usd") is not None else None,
        })
    coll = client[dbname]["paises_big_mac"]
    coll.drop()
    if normalized:
        coll.insert_many(normalized)
    logger.info("Loaded %s documents into paises_big_mac", len(normalized))
    return len(normalized)


def load_costos_turisticos(client, dbname: str, data_dir: Path) -> int:
    files = [
        "costos_turisticos_africa.json",
        "costos_turisticos_america.json",
        "costos_turisticos_asia.json",
        "costos_turisticos_europa.json",
    ]
    all_docs = []
    for filename in files:
        path = data_dir / filename
        if not path.exists():
            logger.warning("Skip missing file: %s", path)
            continue
        with open(path, "r", encoding="utf-8") as f:
            docs = json.load(f)
        if not isinstance(docs, list):
            docs = [docs]
        for d in docs:
            costos = d.get("costos_diarios_estimados_en_dólares") or d.get("costos_diarios_estimados_en_dolares") or {}
            hosp = costos.get("hospedaje") or {}
            comida = costos.get("comida") or {}
            trans = costos.get("transporte") or {}
            entr = costos.get("entretenimiento") or {}
            all_docs.append({
                "continente": d.get("continente"),
                "region": d.get("región", d.get("region")),
                "pais": d.get("país", d.get("pais")),
                "capital": d.get("capital"),
                "poblacion": d.get("población", d.get("poblacion")),
                "costos_diarios_estimados_en_dolares": {
                    "hospedaje": hosp,
                    "comida": comida,
                    "transporte": trans,
                    "entretenimiento": entr,
                },
            })
    coll = client[dbname]["costos_turisticos"]
    coll.drop()
    if all_docs:
        coll.insert_many(all_docs)
    logger.info("Loaded %s documents into costos_turisticos", len(all_docs))
    return len(all_docs)


def main():
    parser = argparse.ArgumentParser(description="Load JSON sources into MongoDB")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=PROJECT_ROOT / "data" / "mongo",
        help="Directory containing Big Mac and costos JSON files",
    )
    args = parser.parse_args()
    data_dir = args.data_dir.resolve()
    if not data_dir.is_dir():
        logger.error("Data dir not found: %s", data_dir)
        sys.exit(1)

    client = None
    try:
        client, dbname = get_client()
        load_big_mac(client, dbname, data_dir)
        load_costos_turisticos(client, dbname, data_dir)
        logger.info("MongoDB source load completed successfully.")
    except Exception as e:
        logger.exception("Load failed: %s", e)
        sys.exit(1)
    finally:
        if client:
            client.close()


if __name__ == "__main__":
    main()
