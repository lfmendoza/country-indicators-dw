from __future__ import annotations

import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def check_duplicate_pais(df: pd.DataFrame, key_col: str = "nombre_pais_normalizado") -> pd.DataFrame:
    n_before = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first")
    n_after = len(df)
    if n_before != n_after:
        logger.warning("Quality: removed %s duplicate rows by %s", n_before - n_after, key_col)
    return df


def check_null_key(df: pd.DataFrame, key_col: str = "nombre_pais_normalizado") -> pd.DataFrame:
    before = len(df)
    df = df[df[key_col].notna() & (df[key_col].astype(str).str.strip() != "")]
    if len(df) != before:
        logger.warning("Quality: removed %s rows with null/empty %s", before - len(df), key_col)
    return df


def check_numeric_ranges(
    df: pd.DataFrame,
    columns_ranges: dict[str, tuple[float | None, float | None]],
) -> pd.DataFrame:
    for col, (lo, hi) in columns_ranges.items():
        if col not in df.columns:
            continue
        s = pd.to_numeric(df[col], errors="coerce")
        if lo is not None:
            below = (s < lo) & s.notna()
            if below.any():
                logger.info("Quality: %s values below %s in %s: %s", below.sum(), lo, col, df.loc[below, col].tolist()[:5])
        if hi is not None:
            above = (s > hi) & s.notna()
            if above.any():
                logger.info("Quality: %s values above %s in %s: %s", above.sum(), hi, col, df.loc[above, col].tolist()[:5])
    return df


def run_quality_checks(df: pd.DataFrame, reject_duplicates: bool = True, require_key: bool = True) -> pd.DataFrame:
    if require_key:
        df = check_null_key(df)
    if reject_duplicates:
        df = check_duplicate_pais(df)
    check_numeric_ranges(df, {
        "tasa_envejecimiento": (0, 100),
        "precio_big_mac_usd": (0, 1000),
        "costo_diario_total_prom_usd": (0, 10000),
        "poblacion": (0, None),
    })
    return df
