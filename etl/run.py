from __future__ import annotations

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from etl.config import get_config, get_country_mapping
from etl.extract import extract_all
from etl.load import load_warehouse
from etl.quality import run_quality_checks
from etl.transform import prepare_for_load, transform_merge

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl.run")


def run_pipeline() -> dict:
    logger.info("Starting ETL pipeline")
    cfg = get_config()
    mapping = get_country_mapping()

    raw = extract_all()
    if not raw:
        raise RuntimeError("Extract returned no data")

    merged = transform_merge(
        raw["pais_poblacion"],
        raw["pais_envejecimiento"],
        raw["big_mac"],
        raw["costos_turisticos"],
        mapping,
    )
    merged = run_quality_checks(
        merged,
        reject_duplicates=cfg.get("quality", {}).get("reject_duplicate_pais", True),
        require_key=cfg.get("quality", {}).get("require_normalized_pais", True),
    )
    prepared = prepare_for_load(merged)

    result = load_warehouse(prepared)
    logger.info("ETL completed: %s", result)
    return {
        "extract": {k: len(v) for k, v in raw.items()},
        "transform": len(prepared),
        "load": result,
    }


if __name__ == "__main__":
    try:
        summary = run_pipeline()
        print("OK", summary)
    except Exception as e:
        logger.exception("Pipeline failed: %s", e)
        sys.exit(1)
