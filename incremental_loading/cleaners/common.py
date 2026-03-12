import pandas as pd
from typing import Dict, Optional


def _default_value_for_dtype(dtype: str):
    """Trả về giá trị mặc định cho column dựa trên dtype khi schema padding.
    
    Args:
        dtype: Type string từ MIN_REQUIRED_SCHEMA (e.g., "Int64", "string", "datetime64[ns]")
    
    Returns:
        Default value phù hợp với dtype:
            - datetime64: pd.NaT (Not a Time)
            - Nullable integers (Int*, UInt*): pd.NA (pandas missing value)
            - Floats (float64, Float64): 0.0
            - string: "" (empty string)
            - Other: pd.NA (safe fallback)
    
    Note:
        - pd.NA là pandas NA type (khác với np.nan)
        - Empty string cho string type để tránh null propagation issues
    """
    if dtype.startswith("datetime64"):
        return pd.NaT

    if dtype in {"Int64", "Int32", "UInt64", "UInt32"}:
        return pd.NA

    if dtype in {"float64", "Float64"}:
        return 0.0

    if dtype == "string":
        return ""

    return pd.NA


def apply_schema_padding(df: pd.DataFrame, required_schema: Dict[str, str]) -> pd.DataFrame:
    """Bổ sung columns thiếu và ép kiểu theo schema tối thiểu đã khai báo.
    
    Args:
        df: DataFrame input (có thể thiếu một số columns hoặc sai type)
        required_schema: Dict mapping column name → dtype string
                        (e.g., {"order_id": "UInt64", "created_at": "datetime64[ns]"})
    
    Returns:
        pd.DataFrame: DataFrame copy với:
            - Tất cả columns trong required_schema (thêm nếu thiếu)
            - Types đã được cast theo required_schema
            - Invalid values được coerce (e.g., "abc" → NaT cho datetime)
    
    Type Conversion Logic:
        
        datetime64:
            - pd.to_datetime() với errors="coerce"
            - Invalid formats → NaT
        
        Integers (Int64, Int32, UInt64, UInt32):
            - pd.to_numeric() → float intermediate
            - Round to remove decimals
            - UInt*: Negative values → NA (unsigned constraint)
            - Cast to nullable integer type (Int64, UInt64, etc.)
        
        Floats (float64, Float64):
            - pd.to_numeric() với errors="coerce"
            - NA → 0.0 (fillna)
            - Cast to float64
        
        string:
            - Fill NA với "" (empty string)
            - Cast to pandas string dtype
    
    Note:
        - Không remove columns ngoài required_schema (extra columns preserved)
        - Nếu column đã tồn tại nhưng sai type, sẽ cast lại
        - UInt types enforce non-negative constraint (negative → NA)
        - Float fillna(0.0) là opinionated choice, có thể customize
    """
    df = df.copy()

    for col, dtype in required_schema.items():
        if col not in df.columns:
            df[col] = _default_value_for_dtype(dtype)

        if dtype.startswith("datetime64"):
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif dtype in {"Int64", "Int32", "UInt64", "UInt32"}:
            numeric_series = pd.to_numeric(df[col], errors="coerce")
            if dtype.startswith("U"):
                numeric_series = numeric_series.where(numeric_series >= 0)
            numeric_series = numeric_series.round()
            df[col] = numeric_series.astype(dtype)
        elif dtype in {"float64", "Float64"}:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype("float64")
        elif dtype == "string":
            df[col] = df[col].fillna("").astype("string")

    return df


def generic_clean(
    df: pd.DataFrame,
    pk_col: str,
    updated_col: str,
    required_schema: Optional[Dict[str, str]] = None,
) -> pd.DataFrame:
    """Apply common data cleaning logic cho tất cả tables (minimum cleaning baseline).
    
    Args:
        df: DataFrame input cần clean
        pk_col: Tên cột primary key (required, không được null)
        updated_col: Tên cột timestamp (required, dùng cho versioning)
        required_schema: Optional schema dict cho padding + type enforcement
    
    Returns:
        pd.DataFrame: DataFrame copy sau khi clean:
            - pk_col: Add với pd.NA nếu thiếu, rows với null PK được drop
            - updated_col: Add với pd.NaT nếu thiếu, parse thành datetime64
            - Schema padding applied nếu required_schema provided
    
    Cleaning Steps:
        
        1. PK Column Handling:
            - Nếu column thiếu: Add column với pd.NA
            - Purpose: Tránh crash khi dropna(subset=[pk_col])
        
        2. Updated Column Handling:
            - Nếu column thiếu: Add column với pd.NaT
            - Parse thành datetime64 (errors="coerce")
            - Invalid timestamps → NaT
        
        3. Schema Padding (optional):
            - Nếu required_schema provided:
                Apply apply_schema_padding() cho type enforcement
        
        4. Drop Invalid Rows:
            - Drop rows với null PK (vì PK là required)
            - Updated column có thể null (kept) 
    
    Custom Cleaners:
        generic_clean() chạy TRƯỚC custom table-specific cleaners.
        Custom cleaners có thể apply business rules:
            - Validate status values
            - Calculate derived columns
            - Apply domain-specific constraints
    
    Note:
        - Copy DataFrame để tránh modify in-place
        - Drop rows với null PK là opinionated (PK required assumption)
        - Updated column null được giữ (có thể có use case cho missing timestamps)
        - required_schema optional để tương thích backward với code cũ
    """
    df = df.copy()

    if pk_col not in df.columns:
        df[pk_col] = pd.NA

    if updated_col not in df.columns:
        df[updated_col] = pd.NaT

    df[updated_col] = pd.to_datetime(df[updated_col], errors="coerce")

    if required_schema:
        df = apply_schema_padding(df, required_schema)

    df = df.dropna(subset=[pk_col])
    return df
