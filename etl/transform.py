from __future__ import annotations

import logging
from typing import Any

import pandas as pd

from etl.config import get_country_mapping

logger = logging.getLogger(__name__)


def normalize_country(name: str, mapping: dict[str, str]) -> str:
    if pd.isna(name) or name is None or str(name).strip() == "":
        return ""
    s = str(name).strip()
    return mapping.get(s) or mapping.get(s.title()) or s


def apply_country_mapping(df: pd.DataFrame, country_col: str, mapping: dict[str, str]) -> pd.DataFrame:
    df = df.copy()
    df["nombre_pais_normalizado"] = df[country_col].apply(lambda x: normalize_country(x, mapping))
    return df


def transform_merge(
    pais_poblacion: pd.DataFrame,
    pais_envejecimiento: pd.DataFrame,
    big_mac: pd.DataFrame,
    costos_turisticos: pd.DataFrame,
    country_mapping: dict[str, str],
) -> pd.DataFrame:
    pp = apply_country_mapping(pais_poblacion, "pais", country_mapping)
    pe = apply_country_mapping(pais_envejecimiento, "nombre_pais", country_mapping)
    bm = apply_country_mapping(big_mac, "pais", country_mapping)
    ct = apply_country_mapping(costos_turisticos, "pais", country_mapping)

    all_paises = set()
    for df, col in [(pp, "nombre_pais_normalizado"), (pe, "nombre_pais_normalizado"), (bm, "nombre_pais_normalizado"), (ct, "nombre_pais_normalizado")]:
        all_paises.update(df[col].dropna().astype(str).str.strip().unique().tolist())
    all_paises.discard("")
    base = pd.DataFrame({"nombre_pais_normalizado": sorted(all_paises)})

    pe_agg = pe.groupby("nombre_pais_normalizado").agg({
        "tasa_de_envejecimiento": "first",
        "capital": "first",
        "continente": "first",
        "region": "first",
    }).reset_index()
    base = base.merge(pe_agg, on="nombre_pais_normalizado", how="left")

    bm_agg = bm[["nombre_pais_normalizado", "precio_big_mac_usd"]].drop_duplicates("nombre_pais_normalizado")
    base = base.merge(bm_agg, on="nombre_pais_normalizado", how="left")

    ct_agg = ct.groupby("nombre_pais_normalizado").agg({
        "poblacion": "first",
        "costo_hospedaje_prom_usd": "first",
        "costo_comida_prom_usd": "first",
        "costo_transporte_prom_usd": "first",
        "costo_entretenimiento_prom_usd": "first",
        "continente": "first",
        "region": "first",
        "capital": "first",
    }).reset_index()
    ct_agg = ct_agg.rename(columns={
        "continente": "continente_ct",
        "region": "region_ct",
        "capital": "capital_ct",
    })
    base = base.merge(ct_agg, on="nombre_pais_normalizado", how="left")
    base["continente"] = base["continente"].fillna(base.get("continente_ct"))
    base["region"] = base["region"].fillna(base.get("region_ct"))
    base["capital"] = base["capital"].fillna(base.get("capital_ct"))
    base = base.drop(columns=[c for c in ["continente_ct", "region_ct", "capital_ct"] if c in base.columns])

    pp_agg = pp.groupby("nombre_pais_normalizado").agg({
        "poblacion": "first",
        "costo_bajo_hospedaje": "first",
        "costo_promedio_comida": "first",
        "costo_bajo_transporte": "first",
        "costo_promedio_entretenimiento": "first",
    }).reset_index()
    base = base.merge(pp_agg, on="nombre_pais_normalizado", how="left", suffixes=("", "_pp"))
    if "poblacion" not in base.columns:
        base["poblacion"] = None
    if "poblacion_pp" in base.columns:
        base["poblacion"] = base["poblacion"].fillna(base["poblacion_pp"])
        base = base.drop(columns=["poblacion_pp"])
    for sql_col, dw_col in [
        ("costo_bajo_hospedaje", "costo_hospedaje_prom_usd"),
        ("costo_promedio_comida", "costo_comida_prom_usd"),
        ("costo_bajo_transporte", "costo_transporte_prom_usd"),
        ("costo_promedio_entretenimiento", "costo_entretenimiento_prom_usd"),
    ]:
        if sql_col in base.columns and dw_col in base.columns:
            base[dw_col] = base[dw_col].fillna(base[sql_col])
        elif sql_col in base.columns:
            base[dw_col] = base[sql_col]

    for c in ["costo_bajo_hospedaje", "costo_promedio_comida", "costo_bajo_transporte", "costo_promedio_entretenimiento"]:
        if c in base.columns:
            base = base.drop(columns=[c])

    if "tasa_de_envejecimiento" in base.columns:
        base["tasa_envejecimiento"] = base["tasa_de_envejecimiento"]
        base = base.drop(columns=["tasa_de_envejecimiento"], errors="ignore")
    else:
        base["tasa_envejecimiento"] = None

    cost_cols = ["costo_hospedaje_prom_usd", "costo_comida_prom_usd", "costo_transporte_prom_usd", "costo_entretenimiento_prom_usd"]
    base["costo_diario_total_prom_usd"] = base[cost_cols].sum(axis=1)
    base.loc[base[cost_cols].isna().all(axis=1), "costo_diario_total_prom_usd"] = None

    if "poblacion" in base.columns:
        base["poblacion"] = pd.to_numeric(base["poblacion"], errors="coerce")
        base["poblacion"] = base["poblacion"].astype("Int64")

    logger.info("Transform merged: %s rows (countries)", len(base))
    return base


def prepare_for_load(merged: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "nombre_pais_normalizado", "continente", "region", "capital",
        "poblacion", "tasa_envejecimiento", "precio_big_mac_usd",
        "costo_hospedaje_prom_usd", "costo_comida_prom_usd",
        "costo_transporte_prom_usd", "costo_entretenimiento_prom_usd",
        "costo_diario_total_prom_usd",
    ]
    out = merged[[c for c in cols if c in merged.columns]].copy()
    return out
