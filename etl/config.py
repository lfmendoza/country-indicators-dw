from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"


def _env(key: str, default: str | None = None) -> str | None:
    return os.environ.get(key) or default


def load_yaml(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_config() -> dict[str, Any]:
    cfg = load_yaml(CONFIG_DIR / "config.yaml")
    pg_cfg = cfg.get("postgres") or {}
    cfg["postgres"] = {
        **pg_cfg,
        "host": _env("POSTGRES_HOST", pg_cfg.get("host", "localhost")),
        "port": int(_env("POSTGRES_PORT", str(pg_cfg.get("port", 5432)))),
        "user": _env("POSTGRES_USER", pg_cfg.get("user", "ci_dw")),
        "password": _env("POSTGRES_PASSWORD", pg_cfg.get("password", "change_me_ci_dw")),
        "dbname": _env("POSTGRES_DB", pg_cfg.get("dbname", "country_indicators_dw")),
        "source_schema": pg_cfg.get("source_schema", "public"),
        "warehouse_schema": pg_cfg.get("warehouse_schema", "warehouse"),
    }
    cfg["mongo"] = {
        **cfg.get("mongo", {}),
        "host": _env("MONGO_HOST", "localhost"),
        "port": int(_env("MONGO_PORT", "27017")),
        "user": _env("MONGO_USER"),
        "password": _env("MONGO_PASSWORD"),
        "dbname": _env("MONGO_DB", "country_indicators_dw"),
    }
    cfg["paths"] = {
        "data_sql": PROJECT_ROOT / cfg.get("paths", {}).get("data_sql", "data/sql"),
        "data_mongo": PROJECT_ROOT / cfg.get("paths", {}).get("data_mongo", "data/mongo"),
    }
    return cfg


def get_country_mapping() -> dict[str, str]:
    path = CONFIG_DIR / "country_mapping.yaml"
    if not path.exists():
        return {}
    data = load_yaml(path)
    return data.get("mapping") or {}
