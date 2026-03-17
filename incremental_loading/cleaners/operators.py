import pandas as pd


def clean_operators(df: pd.DataFrame) -> pd.DataFrame:
    if "name" in df.columns:
        df["name"] = df["name"].fillna("").astype("string").str.strip()

    if "code" in df.columns:
        df["code"] = df["code"].fillna("").astype("string").str.upper().str.strip()

    if "is_deleted" in df.columns:
        df["is_deleted"] = (
            pd.to_numeric(df["is_deleted"], errors="coerce")
            .fillna(0)
            .clip(lower=0, upper=1)
            .round()
            .astype("UInt8")
        )

    if {"is_deleted", "deleted_at", "updated_at"}.issubset(df.columns):
        deleted_mask = df["is_deleted"] == 1
        df.loc[deleted_mask & df["deleted_at"].isna(), "deleted_at"] = df.loc[deleted_mask, "updated_at"]

    return df