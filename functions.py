"""
Helper functions for Medicare Advantage data processing.
Python equivalent of functions.R
"""

import pandas as pd
import numpy as np


def _read_csv_with_fallback(path, **kwargs):
    """
    Read a CSV trying UTF-8 first, then falling back to latin-1.
    """
    try:
        return pd.read_csv(path, encoding="utf-8", **kwargs)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="latin-1", **kwargs)


def read_contract(path):
    col_names = [
        "contractid", "planid", "org_type", "plan_type", "partd", "snp", "eghp",
        "org_name", "org_marketing_name", "plan_name", "parent_org", "contract_date"
    ]
    return _read_csv_with_fallback(
        path,
        skiprows=1,
        names=col_names,
        dtype={
            "contractid": str,
            "planid": float,
            "org_type": str,
            "plan_type": str,
            "partd": str,
            "snp": str,
            "eghp": str,
            "org_name": str,
            "org_marketing_name": str,
            "plan_name": str,
            "parent_org": str,
            "contract_date": str,
        },
    )


def read_enroll(path):
    col_names = ["contractid", "planid", "ssa", "fips", "state", "county", "enrollment"]
    return _read_csv_with_fallback(
        path,
        skiprows=1,
        names=col_names,
        na_values="*",
        dtype={
            "contractid": str,
            "planid": float,
            "ssa": float,
            "fips": float,
            "state": str,
            "county": str,
            "enrollment": float,
        },
    )


def load_month(m, y):
    c_path = f"../ma-data/ma/enrollment/Extracted Data/CPSC_Contract_Info_{y}_{m}.csv"
    e_path = f"../ma-data/ma/enrollment/Extracted Data/CPSC_Enrollment_Info_{y}_{m}.csv"

    contract_info = read_contract(c_path).drop_duplicates(
        subset=["contractid", "planid"], keep="first"
    )
    enroll_info = read_enroll(e_path)

    merged = contract_info.merge(enroll_info, on=["contractid", "planid"], how="left")
    merged["month"] = int(m)
    merged["year"] = y
    return merged


def read_service_area(path):
    col_names = [
        "contractid", "org_name", "org_type", "plan_type", "partial", "eghp",
        "ssa", "fips", "county", "state", "notes"
    ]
    df = _read_csv_with_fallback(
        path,
        skiprows=1,
        names=col_names,
        na_values="*",
        dtype={
            "contractid": str,
            "org_name": str,
            "org_type": str,
            "plan_type": str,
            "partial": str,
            "eghp": str,
            "ssa": float,
            "fips": float,
            "county": str,
            "state": str,
            "notes": str,
        },
    )
    df["partial"] = df["partial"].map({"TRUE": True, "FALSE": False})
    return df


def load_month_sa(m, y):
    path = f"econ470/a0/work/ma-data/ma/service-area/Extracted Data/MA_Cnty_SA_{y}_{m}.csv"
    df = read_service_area(path)
    df["month"] = int(m)
    df["year"] = y
    return df


def read_penetration(path):
    col_names = [
        "state", "county", "fips_state", "fips_cnty", "fips",
        "ssa_state", "ssa_cnty", "ssa", "eligibles", "enrolled", "penetration"
    ]
    df = _read_csv_with_fallback(
        path,
        skiprows=1,
        names=col_names,
        na_values=["", "NA", "*", "-", "--"],
        dtype=str,
    )

    for col in ["eligibles", "enrolled", "penetration"]:
        df[col] = pd.to_numeric(
            df[col].str.replace(",", "").str.replace("%", ""),
            errors="coerce",
        )
    return df


def load_month_pen(m, y):
    path = f"econ470/a0/work/ma-data/ma/penetration/Extracted Data/State_County_Penetration_MA_{y}_{m}.csv"
    df = read_penetration(path)
    df["month"] = int(m)
    df["year"] = y
    return df


def mapd_clean_merge(ma_data, mapd_data, y):
    ma_data = ma_data[["contractid", "planid", "state", "county", "premium"]].copy()

    ma_data = ma_data.sort_values(["contractid", "planid", "state", "county"])
    ma_data["premium"] = ma_data.groupby(
        ["contractid", "planid", "state", "county"]
    )["premium"].ffill()

    ma_data = ma_data.drop_duplicates(
        subset=["contractid", "planid", "state", "county"],
        keep="first"
    )

    mapd_data = mapd_data[
        [
            "contractid", "planid", "state", "county",
            "premium_partc", "premium_partd_basic",
            "premium_partd_supp", "premium_partd_total",
            "partd_deductible",
        ]
    ].copy()

    mapd_data["planid"] = pd.to_numeric(mapd_data["planid"], errors="coerce")

    mapd_data = mapd_data.sort_values(["contractid", "planid", "state", "county"])
    fill_cols = [
        "premium_partc", "premium_partd_basic",
        "premium_partd_supp", "premium_partd_total",
        "partd_deductible",
    ]
    mapd_data[fill_cols] = mapd_data.groupby(
        ["contractid", "planid", "state", "county"]
    )[fill_cols].ffill()

    mapd_data = mapd_data.drop_duplicates(
        subset=["contractid", "planid", "state", "county"],
        keep="first"
    )

    plan_premiums = ma_data.merge(
        mapd_data,
        on=["contractid", "planid", "state", "county"],
        how="outer"
    )
    plan_premiums["year"] = y

    return plan_premiums
