import pandas as pd


def clean_orders(df: pd.DataFrame) -> pd.DataFrame:
    """Table-specific cleaning logic for orders."""
    print("--- Custom cleaning for: ORDERS ---")

    if "total_amount" in df.columns:
        df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0.0)

    if "status" in df.columns:
        df["status"] = df["status"].str.lower().str.strip()

    return df
