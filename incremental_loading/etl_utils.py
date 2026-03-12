"""Shared utilities cho Incremental Loading ETL Pipeline.

Module này cung cấp:
- Context managers cho DB, S3, ClickHouse connections
- Base dataclass cho table configuration
- Checkpoint management (Bronze, Silver, ClickHouse layers)
- Schema evolution detection và notification
- Partitioning utilities (year/month/day)
- S3 file I/O helpers (parquet read/write)

Được dùng bởi:
- oltp_to_minio.py (Bronze ETL)
- bronze_to_silver.py (Silver ETL)
- silver_to_clickhouse.py (ClickHouse sync)
"""
import pandas as pd
import boto3
import json
import os
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
from sqlalchemy import create_engine

from dotenv import load_dotenv

load_dotenv()

# ========== CONTEXT MANAGERS ==========

@contextmanager
def db_session():
    """Context manager cho OLTP database connection lifecycle.
    
    Yields:
        sqlalchemy.Engine: MySQL engine để execute queries
    
    Cleanup:
        Tự động dispose engine khi exit context (release connection pool)
    
    Note:
        Dùng mysqlconnector driver để tương thích với MariaDB.
    """
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
    """Context manager cho S3/MinIO connection lifecycle.
    
    Yields:
        boto3.client: S3 client instance để interact với MinIO/S3
    
    Cleanup:
        Connection tự động close khi exit context
    
    Note:
        endpoint_url cho phép dùng MinIO thay vì AWS S3.
    """
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


@contextmanager
def clickhouse_session():
    """Context manager cho ClickHouse connection lifecycle (HTTP interface).
    
    Yields:
        clickhouse_connect.driver.client.Client: ClickHouse client để execute queries
    
    Cleanup:
        Tự động close connection khi exit context
    
    Raises:
        ImportError: Nếu clickhouse-connect package chưa được install
    
    Note:
        Dùng HTTP interface (port 8123), không phải native protocol (port 9000).
        clickhouse-connect package cần được install: pip install clickhouse-connect
    """
    try:
        import clickhouse_connect
    except ImportError:
        raise ImportError("clickhouse-connect not installed. Run: pip install clickhouse-connect")
    
    client = clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
        database=os.getenv("CLICKHOUSE_DATABASE", "analytics"),
    )
    try:
        print(f"ClickHouse connection established to {os.getenv('CLICKHOUSE_HOST', 'localhost')}")
        yield client
    finally:
        print("ClickHouse connection closed")
        client.close()


# ========== BASE DATACLASS ==========

@dataclass
class BaseTableConfig:
    """Base configuration dataclass cho ETL table configuration.
    
    Attributes:
        table_name: Tên bảng (dùng làm S3 prefix và table identifier)
        pk_col: Tên cột primary key (để deduplication và merge)
        updated_col: Tên cột timestamp (để incremental loading và versioning)
    
    Validation:
        __post_init__ sẽ raise ValueError nếu table_name để trống.
    
    Usage:
        Extend class này để tạo table-specific config:
        - OLTPTableConfig (Bronze layer)
        - TableConfig (Silver layer)
        - ClickHouseTableConfig (ClickHouse layer)
    """
    table_name: str
    pk_col: str = 'id'
    updated_col: str = 'updated_at'
    
    def __post_init__(self):
        """Validation sau khi khởi tạo BaseTableConfig.
        
        Raises:
            ValueError: Nếu table_name để trống
        
        Note:
            pk_col và updated_col có default values nên không bắt buộc validation.
        """
        if not self.table_name:
            raise ValueError("table_name không được để trống")


# ========== CHECKPOINT UTILS ==========

def get_latest_checkpoint_bronze(s3, bucket: str, table_name: str, initial_start: str) -> str:
    """Lấy checkpoint mới nhất từ Bronze layer metadata.
    
    Args:
        s3: Boto3 S3 client instance
        bucket: Bronze bucket name
        table_name: Tên bảng (dùng làm prefix)
        initial_start: Fallback ISO timestamp nếu chưa có metadata
    
    Returns:
        str: ISO format timestamp của last_updated_at từ metadata mới nhất,
             hoặc initial_start nếu chưa có lịch sử.
    
    Metadata Format:
        Bronze layer tạo audit trail với nhiều files:
        - Path: {table_name}/metadata/metadata_YYYYMMDD_HHMMSS.json
        - Find latest by LastModified
        - Extract 'last_updated_at' field
    
    Error Handling:
        Nếu lỗi (bucket không tồn tại, permission denied, etc.):
        - Log warning với exception message
        - Return initial_start (safe fallback)
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
    """Lấy checkpoint mới nhất từ Silver layer metadata.
    
    Args:
        s3: Boto3 S3 client instance
        bucket: Silver bucket name
        table_name: Tên bảng (dùng làm prefix)
        initial_start: Fallback ISO timestamp nếu chưa có metadata
    
    Returns:
        str: ISO format timestamp của last_processed_file_time,
             hoặc initial_start nếu chưa có lịch sử.
    
    Metadata Format:
        Silver layer dùng fixed overwrite file:
        - Path: {table_name}/metadata/silver_checkpoint.json
        - Extract 'last_processed_file_time' field
    
    Error Handling:
        Nếu lỗi (file không tồn tại, permission denied, etc.):
        - Log warning với exception message
        - Return initial_start (safe fallback)
    
    Note:
        last_processed_file_time là LastModified của Bronze file đã process.
    """
    path = f"{table_name}/metadata/silver_checkpoint.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=path)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data.get('last_processed_file_time', initial_start)
    except Exception as e:
        print(f"⚠️  Silver checkpoint not found for {table_name}: {e}. Using default.")
    return initial_start


def get_latest_checkpoint_clickhouse(s3, bucket: str, table_name: str, initial_start: str) -> str:
    """Lấy checkpoint mới nhất từ ClickHouse layer metadata.
    
    Args:
        s3: Boto3 S3 client instance
        bucket: Silver bucket name (ClickHouse metadata lưu trong Silver bucket)
        table_name: Tên bảng (dùng làm prefix)
        initial_start: Fallback ISO timestamp nếu chưa có metadata
    
    Returns:
        str: ISO format timestamp của last_synced_file_time,
             hoặc initial_start nếu chưa có lịch sử.
    
    Metadata Format:
        ClickHouse layer dùng fixed overwrite file:
        - Path: {table_name}/metadata/clickhouse_checkpoint.json
        - Extract 'last_synced_file_time' field
    
    Error Handling:
        Nếu lỗi (file không tồn tại, permission denied, etc.):
        - Log warning với exception message
        - Return initial_start (safe fallback)
    
    Note:
        last_synced_file_time là LastModified của Silver file đã sync vào ClickHouse.
    """
    path = f"{table_name}/metadata/clickhouse_checkpoint.json"
    try:
        obj = s3.get_object(Bucket=bucket, Key=path)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        return data.get('last_synced_file_time', initial_start)
    except Exception as e:
        print(f"⚠️  ClickHouse checkpoint not found for {table_name}: {e}. Using default.")
    return initial_start


def save_checkpoint(s3, bucket: str, table_name: str, metadata_data: dict, layer: str = 'bronze'):
    """Lưu checkpoint metadata lên S3 cho Bronze, Silver, hoặc ClickHouse layer.
    
    Args:
        s3: Boto3 S3 client instance
        bucket: Bucket name để lưu metadata
        table_name: Tên bảng (dùng làm prefix)
        metadata_data: Dictionary chứa checkpoint data và metrics
        layer: 'bronze' | 'silver' | 'clickhouse' (default: bronze)
    
    Raises:
        ValueError: Nếu layer không phải bronze/silver/clickhouse
    
    Layer-Specific Behavior:
        
        Bronze (audit trail):
            - Path: {table_name}/metadata/metadata_{timestamp}.json
            - Mỗi run tạo file mới (append-only history)
            - Metadata fields:
                * last_updated_at: Checkpoint timestamp
                * execution_time: Run duration
                * etl_metrics: oltp_total_rows, duplicate_rows_removed, etc.
                * integrity_check: hash validation results
                * status: SUCCESS | NO_CHANGES | EMPTY_SOURCE
                * schema_columns: Current schema snapshot
        
        Silver (latest state):
            - Path: {table_name}/metadata/silver_checkpoint.json
            - Overwrite file (không giữ history)
            - Metadata fields:
                * last_processed_file_time: LastModified của Bronze file
                * total_records_processed: Cumulative count
                * total_files_processed: File count
                * processed_at: Completion timestamp
                * total_partitions_written: Partition count
                * data_quality_metrics: null counts, rows dropped, etc.
                * schema_columns: Current columns
                * schema_evolution: added/removed columns detection
                * status: SUCCESS | IN_PROGRESS | UP_TO_DATE
        
        ClickHouse (sync tracking):
            - Path: {table_name}/metadata/clickhouse_checkpoint.json
            - Overwrite file
            - Metadata fields:
                * last_synced_file_time: LastModified của Silver file
                * total_records_inserted: Total rows inserted
                * total_files_synced: File count
                * synced_at: Completion timestamp
                * clickhouse_metrics: total_rows_in_table, database, table
                * failed_files: List failed file keys
                * failed_file_details: Error messages + quarantine paths
                * auto_added_columns: Schema migration tracking
    
    Note:
        - Bronze audit trail cho phép historical analysis
        - Silver/ClickHouse overwrite để giảm storage cost
        - IN_PROGRESS status (Silver) hỗ trợ resume khi ETL interrupt
    """
    if layer == 'bronze':
        path = f"{table_name}/metadata/metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    elif layer == 'silver':
        path = f"{table_name}/metadata/silver_checkpoint.json"
    elif layer == 'clickhouse':
        path = f"{table_name}/metadata/clickhouse_checkpoint.json"
    else:
        raise ValueError(f"Invalid layer: {layer}. Must be 'bronze', 'silver' or 'clickhouse'.")
    
    s3.put_object(Bucket=bucket, Key=path, Body=json.dumps(metadata_data, indent=2))


def get_latest_layer_metadata(s3, bucket: str, table_name: str, layer: str) -> dict:
    """Lấy full metadata object mới nhất của một layer (không chỉ checkpoint timestamp).
    
    Args:
        s3: Boto3 S3 client instance
        bucket: Bucket name chứa metadata
        table_name: Tên bảng (dùng làm prefix)
        layer: 'bronze' | 'silver' | 'clickhouse'
    
    Returns:
        dict: Full metadata object chứa checkpoint + metrics + schema,
              hoặc {} nếu chưa có metadata.
    
    Raises:
        ValueError: Nếu layer không hợp lệ
    
    Note:
        - Bronze: Lấy file mới nhất theo LastModified sort
        - Silver/ClickHouse: Đọc fixed filename
        - Empty dict {} là safe default cho first run
    """
    try:
        if layer == 'bronze':
            prefix = f"{table_name}/metadata/metadata_"
            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
            if 'Contents' not in response:
                return {}

            latest_key = sorted(
                response['Contents'],
                key=lambda x: x['LastModified'],
                reverse=True,
            )[0]['Key']

            obj = s3.get_object(Bucket=bucket, Key=latest_key)
            return json.loads(obj['Body'].read().decode('utf-8'))

        if layer == 'silver':
            key = f"{table_name}/metadata/silver_checkpoint.json"
        elif layer == 'clickhouse':
            key = f"{table_name}/metadata/clickhouse_checkpoint.json"
        else:
            raise ValueError(f"Invalid layer: {layer}")

        obj = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(obj['Body'].read().decode('utf-8'))
    except Exception:
        return {}


def diff_schema_columns(previous_columns: Optional[List[str]], current_columns: Optional[List[str]]) -> dict:
    """Phát hiện schema evolution bằng cách so sánh 2 danh sách columns.
    
    Args:
        previous_columns: List columns từ run trước (hoặc None cho first run)
        current_columns: List columns từ run hiện tại (hoặc None nếu không có data)
    
    Returns:
        dict chứa:
            - previous_columns: Sorted list columns cũ (normalized)
            - current_columns: Sorted list columns mới (normalized)
            - added_columns: Sorted list columns được thêm
            - removed_columns: Sorted list columns bị xóa
            - changed: Boolean - True nếu có added hoặc removed
    
    Normalization:
        - Convert tất cả elements sang string
        - Remove duplicates bằng set
        - Sort alphabetically
        - Handle None input as empty list
    
    Note:
        - First run (previous_columns=None) sẽ trả changed=False
        - Case-sensitive comparison
    """
    prev = sorted({str(c) for c in (previous_columns or [])})
    curr = sorted({str(c) for c in (current_columns or [])})

    prev_set = set(prev)
    curr_set = set(curr)

    added = sorted(curr_set - prev_set)
    removed = sorted(prev_set - curr_set)

    return {
        "previous_columns": prev,
        "current_columns": curr,
        "added_columns": added,
        "removed_columns": removed,
        "changed": bool(added or removed),
    }


def notify_schema_event(title: str, message: str, level: str = "WARN") -> bool:
    """Gửi thông báo schema evolution event ra terminal.
    
    Args:
        title: Tiêu đề ngắn của event (e.g., "Schema Evolution Alert - Bronze")
        message: Chi tiết message, có thể multi-line
        level: Mức độ nghiêm trọng - "INFO" | "WARN" | "ERROR" (default: WARN)
    
    Returns:
        bool: True nếu notification được gửi, False nếu được tắt bởi env
    
    Environment Variables:
        SCHEMA_NOTIFY_MODE:
            - "terminal" (default): In ra stdout với timestamp + level
            - "off" | "none" | "0": Tắt hoàn toàn (return False)
    
    Output Format:
        [2026-03-09 15:20:30] [WARN] Schema Evolution Alert - Bronze
          Table: orders
          Added columns: ['discount', 'tax_amount']
          Removed columns: []
          Layer: Bronze
    
    Example:
        notify_schema_event(
            title="Schema Evolution Alert - Silver",
            message="Table: orders\nAdded: ['discount']\nLayer: Silver",
            level="WARN"
        )
    
    Note:
        - Multi-line messages được indent 2 spaces cho mỗi dòng
        - Timestamp format: YYYY-MM-DD HH:MM:SS
        - Level không validate, có thể dùng custom levels
    """
    mode = os.getenv("SCHEMA_NOTIFY_MODE", "terminal").strip().lower()
    if mode in {"off", "none", "0"}:
        return False

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {title}")
    for line in str(message).splitlines():
        print(f"  {line}")
    return True


# ========== PARTITIONING UTILS ==========

def add_partitions(df: pd.DataFrame, time_col: str) -> pd.DataFrame:
    """Thêm các cột partitioning (year, month, day) vào DataFrame cho Hive-style partitioning.
    
    Args:
        df: DataFrame input chứa time column
        time_col: Tên cột timestamp để extract partition keys
    
    Returns:
        pd.DataFrame: DataFrame copy với 3 cột thêm:
            - 'y': Year (int, e.g., 2026)
            - 'm': Month (int, e.g., 3)
            - 'd': Day (int, e.g., 9)
    
    Raises:
        ValueError: Nếu time_col không tồn tại trong DataFrame
    
    Process:
        1. Copy DataFrame để tránh modify in-place
        2. Convert time_col sang datetime64 (nếu chưa phải)
        3. Extract year, month, day vào cột mới
    
    Note:
        - Cột partitioning (y, m, d) thường được drop trước khi write final data
        - Month/day không zero-padded trong column value (3 chứ không phải "03")
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
    """Sinh Hive-style partition paths và split DataFrame theo partitions.
    
    Args:
        df: DataFrame đã có cột partitioning (y, m, d) từ add_partitions()
        table_name: Tên bảng (dùng làm root prefix)
        suffix: File suffix bao gồm leading '/' (default: "/data.parquet")
    
    Returns:
        List[Tuple[str, pd.DataFrame]]: Mỗi tuple chứa:
            - path: S3 key theo format Hive partitioning
            - df_part: DataFrame subset cho partition đó
    
    Path Format:
        {table_name}/year={y}/month={mm}/day={dd}{suffix}
        Ví dụ: orders/year=2026/month=03/day=09/data.parquet
    
    Process:
        1. Find unique combinations của (y, m, d)
        2. For each partition:
            - Build Hive-style path (month/day zero-padded)
            - Filter DataFrame rows thuộc partition đó
            - Add (path, df_part) vào result list
    
    Note:
        - Month/day được zero-padded trong path (03, 09) cho consistency
        - DataFrame phải có columns y, m, d (gọi add_partitions() trước)
        - suffix phải include leading '/' nếu là file path
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
    """Read parquet file từ S3 vào pandas DataFrame.
    
    Args:
        s3: Boto3 S3 client instance
        bucket: S3 bucket name
        key: S3 object key của parquet file
    
    Returns:
        pd.DataFrame: DataFrame chứa data từ parquet file
    
    Raises:
        ClientError: Nếu file không tồn tại hoặc permission denied
        ParserError: Nếu file không phải parquet hợp lệ
    
    Process:
        1. Download file content vào memory (BytesIO buffer)
        2. Parse bằng pd.read_parquet()
    
    Note:
        - File được load toàn bộ vào memory, không streaming
        - Cẩn thận với files lớn (> 1GB)
    """
    from io import BytesIO
    resp = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_parquet(BytesIO(resp['Body'].read()))


def write_parquet_to_s3(s3, bucket: str, key: str, df: pd.DataFrame):
    """Write pandas DataFrame lên S3 dưới dạng parquet file.
    
    Args:
        s3: Boto3 S3 client instance
        bucket: S3 bucket name
        key: S3 object key để write
        df: DataFrame cần write
    
    Raises:
        ClientError: Nếu permission denied hoặc bucket không tồn tại
    
    Process:
        1. Serialize DataFrame sang parquet bytes (in-memory buffer)
        2. Upload buffer content lên S3 bằng put_object()
    
    Note:
        - Index không được write (index=False)
        - Parquet compression dùng default (thường là snappy)
        - Overwrite nếu key đã tồn tại (S3 put_object behavior)
    """
    from io import BytesIO
    buf = BytesIO()
    df.to_parquet(buf, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
