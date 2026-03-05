import pandas as pd


def clean_users(df: pd.DataFrame) -> pd.DataFrame:
    """Table-specific cleaning logic for users."""
    if "email" in df.columns:
        df["email"] = df["email"].str.lower()
    return df
