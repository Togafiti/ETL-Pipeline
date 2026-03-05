import pandas as pd


def generic_clean(df: pd.DataFrame, pk_col: str, updated_col: str) -> pd.DataFrame:
    """Apply common cleaning used by all tables."""
    df = df.copy()
    df[updated_col] = pd.to_datetime(df[updated_col])
    df = df.dropna(subset=[pk_col])
    return df
