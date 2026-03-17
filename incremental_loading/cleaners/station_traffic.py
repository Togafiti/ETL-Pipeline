import pandas as pd


VALID_TECHNOLOGIES = {"3G", "4G", "5G"}


def clean_station_traffic(df: pd.DataFrame) -> pd.DataFrame:
    if "technology" in df.columns:
        df["technology"] = df["technology"].fillna("UNKNOWN").astype("string").str.upper().str.strip()
        df.loc[~df["technology"].isin(VALID_TECHNOLOGIES), "technology"] = "UNKNOWN"

    if "traffic_gb" in df.columns:
        df["traffic_gb"] = pd.to_numeric(df["traffic_gb"], errors="coerce").fillna(0.0).clip(lower=0.0)

    if "user_count" in df.columns:
        df["user_count"] = (
            pd.to_numeric(df["user_count"], errors="coerce")
            .fillna(0)
            .clip(lower=0)
            .round()
            .astype("UInt32")
        )

    if "event_time" in df.columns:
        event_time = pd.to_datetime(df["event_time"], errors="coerce")
        if "updated_at" in df.columns:
            event_time = event_time.fillna(pd.to_datetime(df["updated_at"], errors="coerce"))
        if "created_at" in df.columns:
            event_time = event_time.fillna(pd.to_datetime(df["created_at"], errors="coerce"))
        df["event_time"] = event_time

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