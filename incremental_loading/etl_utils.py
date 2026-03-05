"""
Shared utilities cho ETL pipeline (OLTP->Bronze & Bronze->Silver)
"""
import pandas as pd
import boto3
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Callable
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

# ========== CONTEXT MANAGERS ==========

@contextmanager
def db_session():
    """Context manager tự động setup/cleanup DB connection"""
    engine = create_engine(
        f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
    )
    try:
        print("Database connection established")
        yield engine
    finally:
        print("Database connection closed")
        engine.dispose()


@contextmanager
def s3_session():
    """Context manager tự động setup/cleanup S3 connection"""
    s3_client = boto3.client("s3", 
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url=os.getenv("AWS_ENDPOINT_URL")
    )
    try:
        print("S3 connection established")
        yield s3_client
    finally:
        print("S3 connection closed")


# ========== BASE DATACLASS ==========

@dataclass
class BaseTableConfig:
    """Base config cho mỗi bảng trong pipeline ETL"""
    table_name: str
    pk_col: str = 'id'
    updated_col: str = 'updated_at'
    
    def __post_init__(self):
        """Validation sau init"""
        if not self.table_name:
            raise ValueError("table_name không được để trống")


# ========== CHECKPOINT UTILS ==========

def get_latest_checkpoint_bronze(s3, bucket: str, table_name: str, initial_start: str) -> str:
    """
    Lấy checkpoint mới nhất từ S3 metadata cho Bronze layer.
    Đọc từ prefix: {table_name}/metadata/metadata_*
    Trả về ISO format timestamp.
    """
    prefix = f"{table_name}/metadata/metadata_"
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        if 'Contents' in response:
            latest_file = sorted(
                response['Contents'], 
                key=lambda x: x['LastModified'], 
                reverse=True
            )[0]['Key']
            obj = s3.get_object(Bucket=bucket, Key=latest_file)
            data = json.loads(obj['Body'].read().decode('utf-8'))
            return data.get('last_updated_at', initial_start)
    except Exception as e:
        print(f"⚠️  Bronze checkpoint not found for {table_name}: {e}. Using default.")
    return initial_start


def get_latest_checkpoint_silver(s3, bucket: str, table_name: str, initial_start: str) -> str:
    """
    Lấy checkpoint mới nhất từ S3 metadata cho Silver layer.
    Đọc từ file: {table_name}/metadata/silver_checkpoint.json
    Trả về ISO format timestamp.
    """
    path = f"{table_name}/metadata/silver_checkpoint.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=path)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data.get('last_processed_file_time', initial_start)
    except Exception as e:
        print(f"⚠️  Silver checkpoint not found for {table_name}: {e}. Using default.")
    return initial_start


def save_checkpoint(s3, bucket: str, table_name: str, metadata_data: dict, layer: str = 'bronze'):
    """
    Lưu checkpoint lên S3 cho Bronze hoặc Silver layer.
    
    Bronze metadata format (audit trail):
        - Tạo file mới mỗi lần: metadata_YYYYMMDD_HHMMSS.json
        - Chứa: last_updated_at, execution_time, etl_metrics, integrity_check, status
    
    Silver metadata format (quality tracking):
        - Overwrite file cố định: silver_checkpoint.json
        - Chứa: last_processed_file_time, total_records_processed, total_files_processed,
                processed_at, total_partitions_written, data_quality_metrics
    """
    if layer == 'bronze':
        path = f"{table_name}/metadata/metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    elif layer == 'silver':
        path = f"{table_name}/metadata/silver_checkpoint.json"
    else:
        raise ValueError(f"Invalid layer: {layer}. Must be 'bronze' or 'silver'.")
    
    s3.put_object(Bucket=bucket, Key=path, Body=json.dumps(metadata_data, indent=2))


# ========== PARTITIONING UTILS ==========

def add_partitions(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    """
    Thêm cột partitioning (year, month, day) vào DataFrame.
    
    Args:
        df: DataFrame input
        time_col: Tên cột thời gian để partitioning
    
    Returns:
        DataFrame với 3 cột thêm: 'y', 'm', 'd'
    """
    if time_col not in df.columns:
        raise ValueError(f"Cột '{time_col}' không tồn tại trong DataFrame")
    
    df = df.copy()
    df[time_col] = pd.to_datetime(df[time_col])
    df['y'] = df[time_col].dt.year
    df['m'] = df[time_col].dt.month
    df['d'] = df[time_col].dt.day
    return df


def get_partition_paths(df: pd.DataFrame, table_name: str, suffix: str = "/data.parquet") -> list:
    """
    Sinh danh sách partition paths từ DataFrame đã có cột y, m, d.
    
    Args:
        df: DataFrame (phải có cột y, m, d)
        table_name: Tên bảng
        suffix: File suffix, include '/' nếu là file path (ví dụ: "/clean_data.parquet")
    
    Returns:
        List of (path, df_part) tuples
    """
    partitions = df[['y', 'm', 'd']].drop_duplicates().values.tolist()
    paths = []
    
    for y, m, d in partitions:
        path = f"{table_name}/year={y}/month={m:02d}/day={d:02d}{suffix}"
        df_part = df[(df['y'] == y) & (df['m'] == m) & (df['d'] == d)]
        paths.append((path, df_part))
    
    return paths


# ========== S3 FILE I/O UTILS ==========

def read_parquet_from_s3(s3, bucket: str, key: str) -> pd.DataFrame:
    """Đọc parquet file từ S3"""
    from io import BytesIO
    resp = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(resp['Body'].read()))


def write_parquet_to_s3(s3, bucket: str, key: str, df: pd.DataFrame):
    """Ghi parquet file lên S3"""
    from io import BytesIO
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
