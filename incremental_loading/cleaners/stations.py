import pandas as pd


def clean_stations(df: pd.DataFrame) -> pd.DataFrame:
    if "station_code" in df.columns:
        df["station_code"] = df["station_code"].fillna("").astype("string").str.upper().str.strip()

    for col in ["province", "district"]:
        if col in df.columns:
            df[col] = df[col].fillna("UNKNOWN").astype("string").str.strip()
            df.loc[df[col] == "", col] = "UNKNOWN"

    if "status" in df.columns:
        df["status"] = df["status"].fillna("active").astype("string").str.lower().str.strip()
        df.loc[~df["status"].isin(["active", "inactive"]), "status"] = "active"

    if "latitude" in df.columns:
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce").fillna(0.0).clip(-90.0, 90.0)

    if "longitude" in df.columns:
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce").fillna(0.0).clip(-180.0, 180.0)

    if "install_date" in df.columns:
        df["install_date"] = pd.to_datetime(df["install_date"], errors="coerce")

    if "is_deleted" in df.columns:
        df["is_deleted"] = (
            pd.to_numeric(df["is_deleted"], errors="coerce")
            .fillna(0)
            .clip(lower=0, upper=1)
            .round()
            .astype("UInt8")
        )

    if {"is_deleted", "status"}.issubset(df.columns):
        df.loc[df["is_deleted"] == 1, "status"] = "inactive"

    if {"is_deleted", "deleted_at", "updated_at"}.issubset(df.columns):
        deleted_mask = df["is_deleted"] == 1
        df.loc[deleted_mask & df["deleted_at"].isna(), "deleted_at"] = df.loc[deleted_mask, "updated_at"]

    return df