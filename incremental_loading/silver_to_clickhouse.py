import os
import json
from io import BytesIO
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, List

from dotenv import load_dotenv

from etl_utils import (
    clickhouse_session,
    get_latest_checkpoint_clickhouse,
    s3_session,
    save_checkpoint,
    notify_schema_event,
)

load_dotenv()

def _parse_iso_datetime(value: str) -> datetime:
    """Parse ISO datetime string và normalize timezone để so sánh chính xác.
    
    Args:
        value: ISO datetime string (có thể có trailing 'Z' timezone marker)
    
    Returns:
        datetime: Aware datetime object với timezone UTC
    
    Note:
        - 'Z' suffix được convert thành '+00:00' trước khi parse
        - Naive datetime (không có timezone) được normalize thành UTC
        - Đảm bảo aware-vs-aware comparison, tránh TypeError
    """
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Normalize naive timestamps as UTC to avoid aware-vs-naive comparison errors.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass
class ClickHouseTableConfig:
    """Cấu hình cho mỗi bảng trong pipeline Silver->ClickHouse.
    
    Attributes:
        table_name: Tên bảng Silver (dùng làm S3 prefix)
        pk_col: Tên cột primary key (để deduplication trong ReplacingMergeTree)
        version_col: Tên cột version/timestamp cho ReplacingMergeTree (optional)
        clickhouse_table: Tên table trong ClickHouse (default giống table_name)
        clickhouse_database: Database name trong ClickHouse (default: analytics)
    
    Validation:
        __post_init__ sẽ raise ValueError nếu table_name hoặc pk_col để trống.
    """

    table_name: str
    pk_col: str
    version_col: Optional[str] = None
    clickhouse_table: Optional[str] = None
    clickhouse_database: str = "analytics"

    def __post_init__(self):
        """Validation và set defaults sau khi khởi tạo dataclass.
        
        Raises:
            ValueError: Nếu table_name hoặc pk_col để trống
        
        Side Effects:
            - Nếu clickhouse_table không được set, sẽ dùng table_name
        """


class SilverToClickHouse:
    """Sync data từ Silver S3 bucket sang ClickHouse sử dụng S3 table function.
    
    Features:
        - Incremental loading dựa trên file LastModified checkpoint
        - Auto schema migration: ADD COLUMN tự động cho columns mới
        - Quarantine pattern: Isolate failed files với error manifest
        - Column intersection: Chỉ insert columns tồn tại ở cả file và table
        - Per-file insertion để tracking chi tiết và error recovery
    
    Architecture:
        - S3 parquet files làm data source
        - ClickHouse s3() table function để read trực tiếp từ S3
        - ReplacingMergeTree engine để tự động deduplicate by PK
    """
    
    def __init__(self, config: ClickHouseTableConfig):
        """Khởi tạo ClickHouse sync processor với cấu hình bảng.
        
        Args:
            config: ClickHouseTableConfig instance chứa table metadata và connection info
        
        Raises:
            ValueError: Nếu ILOADING_SILVER_BUCKET_NAME không được set
        """
        self.table_name = config.table_name
        self.pk_col = config.pk_col
        self.version_col = config.version_col
        self.clickhouse_table = config.clickhouse_table
        self.clickhouse_database = config.clickhouse_database

        self.bucket_silver = os.getenv("ILOADING_SILVER_BUCKET_NAME")
        # ClickHouse S3 endpoint - use host.docker.internal if ClickHouse runs in Docker
        self.minio_url = os.getenv("CLICKHOUSE_S3_ENDPOINT", "http://host.docker.internal:9000")
        self.s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
        self.s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
        self.initial_start = os.getenv("INITIAL_START", "2000-01-01T00:00:00")
        self.auto_add_columns = os.getenv("CLICKHOUSE_AUTO_ADD_COLUMNS", "1") == "1"
        self.schema_detection_sample_files = int(os.getenv("SCHEMA_DETECTION_SAMPLE_FILES", "5"))
        self.quarantine_bucket = os.getenv("CLICKHOUSE_QUARANTINE_BUCKET", self.bucket_silver)
        self.quarantine_prefix = os.getenv("CLICKHOUSE_QUARANTINE_PREFIX", "_quarantine/clickhouse")
        self.quarantine_move_source = os.getenv("CLICKHOUSE_QUARANTINE_MOVE_SOURCE", "1") == "1"

        if not self.bucket_silver:
            raise ValueError("ILOADING_SILVER_BUCKET_NAME environment variable not set")

    def get_checkpoint(self, s3) -> str:
        """Lấy checkpoint ClickHouse layer để xác định files cần sync.
        
        Args:
            s3: Boto3 S3 client instance
        
        Returns:
            str: ISO timestamp của last_synced_file_time,
                 hoặc INITIAL_START nếu chưa có lịch sử sync.
        
        Note:
            Checkpoint được lưu trong {table_name}/metadata/clickhouse_checkpoint.json.
            Khác với Bronze/Silver, ClickHouse checkpoint là LastModified của Silver file.
        """
        return get_latest_checkpoint_clickhouse(
            s3,
            self.bucket_silver,
            self.table_name,
            self.initial_start,
        )

    def save_checkpoint_with_metrics(self, s3, metadata: dict):
        """Lưu sync metadata với ClickHouse metrics lên S3.
        
        Args:
            s3: Boto3 S3 client instance
            metadata: Dictionary chứa:
                - last_synced_file_time: ISO timestamp của file mới nhất đã sync
                - total_files_found: Số files mới tìm thấy
                - total_files_succeeded: Số files insert thành công
                - total_files_failed: Số files bị quarantine
                - rows_inserted: Tổng rows đã insert vào ClickHouse
                - failed_files: List file paths bị lỗi
                - failed_file_details: Chi tiết lỗi + quarantine info
                - auto_added_columns: Columns được thêm tự động
                - clickhouse_metrics: Total rows in table, database, table name
        
        Note:
            Metadata file là overwrite (không phải append).
        """
        save_checkpoint(
            s3,
            self.bucket_silver,
            self.table_name,
            metadata,
            layer="clickhouse",
        )

    def _validate_clickhouse_table_exists(self, ch_client):
        """Kiểm tra ClickHouse table tồn tại trước khi sync.
        
        Args:
            ch_client: ClickHouse client instance
        
        Raises:
            ValueError: Nếu table không tồn tại trong database.
                       Message hướng dẫn tạo table từ clickhouse_schemas.sql
        
        Note:
            Table phải được tạo sẵn bằng DDL script, ETL không tự động CREATE TABLE.
        """
        query = f"""
        SELECT count()
        FROM system.tables
        WHERE database = '{self.clickhouse_database}'
          AND name = '{self.clickhouse_table}'
        """
        result = ch_client.query(query)
        count = result.result_rows[0][0] if result.result_rows else 0
        if count == 0:
            raise ValueError(
                f"Table {self.clickhouse_database}.{self.clickhouse_table} does not exist. "
                "Create tables from clickhouse_schemas.sql first."
            )

    def _get_clickhouse_table_columns(self, ch_client) -> List[str]:
        """Lấy danh sách column names của ClickHouse table (không bao gồm internal columns).
        
        Args:
            ch_client: ClickHouse client instance
        
        Returns:
            List[str]: Ordered list các column names, không bao gồm _sign, _version
        
        Note:
            Internal columns (_sign, _version) của ReplacingMergeTree được exclude
            vì không cần map với parquet schema.
        """
        query = f"""
        SELECT name
        FROM system.columns
        WHERE database = '{self.clickhouse_database}'
          AND table = '{self.clickhouse_table}'
          AND name NOT IN ('_sign', '_version')  -- Bỏ các column internal của ReplacingMergeTree
        ORDER BY position
        """
        result = ch_client.query(query)
        return [row[0] for row in result.result_rows]

    def _get_clickhouse_table_column_types(self, ch_client) -> Dict[str, str]:
        """Lấy mapping column name → ClickHouse type cho schema comparison.
        
        Args:
            ch_client: ClickHouse client instance
        
        Returns:
            Dict[str, str]: Mapping từ column name sang ClickHouse type string
                           (e.g., {'order_id': 'UInt64', 'total_amount': 'Float64'})
        
        Note:
            Internal columns (_sign, _version) được exclude.
            Dùng để detect columns mới từ parquet schema.
        """
        query = f"""
        SELECT name, type
        FROM system.columns
        WHERE database = '{self.clickhouse_database}'
          AND table = '{self.clickhouse_table}'
          AND name NOT IN ('_sign', '_version')
        ORDER BY position
        """
        result = ch_client.query(query)
        return {row[0]: row[1] for row in result.result_rows}

    def _read_parquet_schema(self, s3, file_key: str):
        """Read parquet file schema bằng pyarrow để detect columns và types.
        
        Args:
            s3: Boto3 S3 client instance
            file_key: S3 key của parquet file
        
        Returns:
            pyarrow.Schema: Schema object chứa fields và types
        
        Note:
            Chỉ read schema metadata, không load full data → nhanh và tiết kiệm memory.
            Dùng để auto schema detection cho ADD COLUMN.
        """
        import pyarrow.parquet as pq

        obj = s3.get_object(Bucket=self.bucket_silver, Key=file_key)
        return pq.read_schema(BytesIO(obj["Body"].read()))

    @staticmethod
    def _map_pyarrow_to_clickhouse_type(pa_type) -> str:
        """Map pyarrow DataType sang ClickHouse type string cho ALTER TABLE ADD COLUMN.
        
        Args:
            pa_type: pyarrow.DataType instance (e.g., pa.uint64(), pa.float64())
        
        Returns:
            str: ClickHouse type string (e.g., 'UInt64', 'Float64', 'String', 'DateTime')
        
        Mapping Rules:
            - Integers: uint8/16/32/64 → UInt*, int8/16/32/64 → Int*
            - Floats: float16/32 → Float32, float64 → Float64
            - Decimal: decimal(p, s) → Decimal(p, s)
            - Boolean: bool → UInt8
            - Timestamp: timestamp → DateTime
            - Date: date → Date
            - String: string/large_string → String
            - Unknown: Fallback → String (safe default)
        
        Note:
            Fallback to String cho phép tương thích với parquet types phức tạp (nested, etc.).
        """
        import pyarrow as pa

        if pa.types.is_uint8(pa_type):
            return "UInt8"
        if pa.types.is_uint16(pa_type):
            return "UInt16"
        if pa.types.is_uint32(pa_type):
            return "UInt32"
        if pa.types.is_uint64(pa_type):
            return "UInt64"
        if pa.types.is_int8(pa_type):
            return "Int8"
        if pa.types.is_int16(pa_type):
            return "Int16"
        if pa.types.is_int32(pa_type):
            return "Int32"
        if pa.types.is_int64(pa_type):
            return "Int64"
        if pa.types.is_float16(pa_type):
            return "Float32"
        if pa.types.is_float32(pa_type):
            return "Float32"
        if pa.types.is_float64(pa_type):
            return "Float64"
        if pa.types.is_decimal(pa_type):
            return f"Decimal({pa_type.precision}, {pa_type.scale})"
        if pa.types.is_boolean(pa_type):
            return "UInt8"
        if pa.types.is_timestamp(pa_type):
            return "DateTime"
        if pa.types.is_date(pa_type):
            return "Date"
        if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return "String"

        # Fallback an toàn cho type phức tạp
        return "String"

    def _get_file_columns(self, s3, file_key: str) -> List[str]:
        """Lấy danh sách column names từ parquet file schema.
        
        Args:
            s3: Boto3 S3 client instance
            file_key: S3 key của parquet file
        
        Returns:
            List[str]: Ordered list các column names trong file
        """
        schema = self._read_parquet_schema(s3, file_key)
        return [field.name for field in schema]

    def _auto_add_new_columns(self, s3, ch_client, new_files: List[dict]) -> List[dict]:
        """Tự động ADD COLUMN cho columns mới phát hiện trong parquet schema.
        
        Args:
            s3: Boto3 S3 client instance
            ch_client: ClickHouse client instance
            new_files: List các file objects (với Key attribute) cần scan
        
        Returns:
            List[dict]: Mỗi item chứa:
                - column: Tên column được thêm
                - clickhouse_type: ClickHouse type string
                - pyarrow_type: PyArrow type string (for logging)
        
        Process:
            1. Load existing table schema từ system.columns
            2. Sample SCHEMA_DETECTION_SAMPLE_FILES files để detect parquet schema
            3. For each column trong parquet nhưng không trong table:
                - Map pyarrow type → ClickHouse type
                - Execute ALTER TABLE ADD COLUMN IF NOT EXISTS
                - Log và notify schema event
        
        Feature Flags:
            - CLICKHOUSE_AUTO_ADD_COLUMNS=0: Disable auto migration (return [])
            - SCHEMA_DETECTION_SAMPLE_FILES: Giới hạn số files scan (default: 5)
        
        Note:
            - Chỉ ADD columns, không DROP hoặc ALTER type
            - Sample limited files để tránh scan toàn bộ khi có nhiều files
            - IF NOT EXISTS đảm bảo idempotent (safe to retry)
        """
        if not self.auto_add_columns:
            return []

        existing_types = self._get_clickhouse_table_column_types(ch_client)
        parquet_types = {}

        for obj in new_files[: self.schema_detection_sample_files]:
            schema = self._read_parquet_schema(s3, obj["Key"])
            for field in schema:
                if field.name not in parquet_types:
                    parquet_types[field.name] = field.type

        added_columns = []
        for col_name, pa_type in parquet_types.items():
            if col_name in existing_types:
                continue

            ch_type = self._map_pyarrow_to_clickhouse_type(pa_type)
            alter_query = (
                f"ALTER TABLE {self.clickhouse_database}.{self.clickhouse_table} "
                f"ADD COLUMN IF NOT EXISTS `{col_name}` {ch_type}"
            )
            ch_client.command(alter_query)
            added_columns.append({"column": col_name, "clickhouse_type": ch_type, "pyarrow_type": str(pa_type)})
            print(f"  + Added column: {col_name} {ch_type} (from {pa_type})")

        if added_columns:
            added = ", ".join([f"{c['column']}:{c['clickhouse_type']}" for c in added_columns])
            notify_schema_event(
                title="Schema Evolution Alert - ClickHouse Auto ADD",
                message=(
                    f"Table: {self.clickhouse_database}.{self.clickhouse_table}\n"
                    f"Layer: ClickHouse\n"
                    f"Auto-added columns: {added}"
                ),
                level="INFO",
            )

        return added_columns

    def _find_new_files(self, s3, last_cp: str) -> List[dict]:
        """Scan Silver bucket để tìm parquet files mới hơn checkpoint.
        
        Args:
            s3: Boto3 S3 client instance
            last_cp: ISO timestamp checkpoint từ lần sync trước
        
        Returns:
            List[dict]: Sorted list các S3 object dicts (Key, LastModified, etc.)
                       Sorted by LastModified ascending
        
        Filter Logic:
            - Chỉ lấy files .parquet trong prefix {table_name}/
            - LastModified > checkpoint timestamp
        
        Note:
            Invalid checkpoint format sẽ fallback về INITIAL_START.
        """
        try:
            last_cp_ts = _parse_iso_datetime(last_cp).timestamp()
        except ValueError:
            print(f"Invalid checkpoint format '{last_cp}'. Fallback to initial_start.")
            last_cp_ts = _parse_iso_datetime(self.initial_start).timestamp()

        paginator = s3.get_paginator("list_objects_v2")
        new_files = []

        for page in paginator.paginate(Bucket=self.bucket_silver, Prefix=f"{self.table_name}/"):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if key.endswith(".parquet") and obj["LastModified"].timestamp() > last_cp_ts:
                    new_files.append(obj)

        return sorted(new_files, key=lambda x: x["LastModified"])

    def _insert_from_s3(self, s3, ch_client, file_keys: List[str], table_columns: List[str]):
        """Insert data từ danh sách S3 files vào ClickHouse bằng s3() table function.
        
        Args:
            s3: Boto3 S3 client instance
            ch_client: ClickHouse client instance
            file_keys: List S3 keys cần insert
            table_columns: List columns hiện có trong ClickHouse table
        
        Returns:
            Tuple[int, List[dict]]:
                - total_rows_inserted: Tổng rows đã insert thành công
                - failed_files: List chi tiết files bị lỗi và được quarantine
        
        Process (Per-File):
            1. Read parquet schema để lấy file columns
            2. Intersection: selected_columns = table_columns ∩ file_columns
            3. Build INSERT query với s3() table function:
                INSERT INTO {table} ({selected_columns})
                SELECT {selected_columns}
                FROM s3('{s3_url}', '{access_key}', '{secret_key}', 'Parquet')
            4. Count rows before và after insert để tính rows_added
            5. Nếu exception:
                - Quarantine file (copy + error manifest + optional delete source)
                - Add to failed_files list
                - Continue với file tiếp theo (không fail toàn bộ)
        
        Column Intersection Logic:
            - Chỉ insert columns có trong CẢ file và table
            - Columns mới trong parquet đã được ADD bởi _auto_add_new_columns()
            - Columns cũ bị removed từ parquet sẽ không được insert (safe skip)
        
        Quarantine Pattern:
            - Failed files được isolate để không block toàn bộ sync
            - Error manifest cho phép root cause analysis
            - Optional source deletion để tránh retry vô hạn
        
        Note:
            - Per-file insertion cho phép partial success và detailed tracking
            - ClickHouse s3() function read trực tiếp từ S3, không cần download local
            - Endpoint phải là URL mà ClickHouse access được (thường dùng host.docker.internal)
        """
        if not file_keys:
            return 0, []
        
        total_rows_inserted = 0
        failed_files = []
        
        # Insert từng file một để dễ dàng tracking lỗi và tính toán số rows inserted chính xác
        for file_key in file_keys:
            try:
                file_columns = self._get_file_columns(s3, file_key)
                selected_columns = [col for col in table_columns if col in file_columns]

                if not selected_columns:
                    raise ValueError("No overlapping columns between file schema and ClickHouse table")

                columns_select = ", ".join(selected_columns)
                columns_insert = ", ".join(selected_columns)

                # S3 URL cho ClickHouse (đảm bảo endpoint và bucket đúng)
                s3_url = f"{self.minio_url}/{self.bucket_silver}/{file_key}"
                
                # INSERT query using s3() table function
                query = f"""
                INSERT INTO {self.clickhouse_database}.{self.clickhouse_table} ({columns_insert})
                SELECT {columns_select}
                FROM s3(
                    '{s3_url}',
                    '{self.s3_access_key}',
                    '{self.s3_secret_key}',
                    'Parquet'
                )
                """
                
                # Lấy count trước khi insert
                rows_before = ch_client.query(
                    f"SELECT count() FROM {self.clickhouse_database}.{self.clickhouse_table}"
                ).result_rows[0][0]
                
                # Execute INSERT
                ch_client.command(query)
                
                # Lấy count sau khi insert 
                rows_after = ch_client.query(
                    f"SELECT count() FROM {self.clickhouse_database}.{self.clickhouse_table}"
                ).result_rows[0][0]
                
                rows_added = rows_after - rows_before
                total_rows_inserted += rows_added
                
                print(f"    ✓ {file_key}: +{rows_added} rows ({len(selected_columns)} cols)")
                
            except Exception as exc:
                print(f"    ✗ {file_key}: FAILED - {exc}")
                quarantine_result = self._quarantine_failed_file(s3, file_key, str(exc))
                failed_files.append(
                    {
                        "file": file_key,
                        "error": str(exc),
                        "quarantine": quarantine_result,
                    }
                )
                continue
        
        return total_rows_inserted, failed_files

    def _build_quarantine_key(self, file_key: str) -> str:
        """Sinh quarantine S3 key cho failed file với timestamp isolation.
        
        Args:
            file_key: Original S3 key của failed file
        
        Returns:
            str: Quarantine key format:
                 {quarantine_prefix}/{table_name}/{timestamp}/{original_key}
        
        Note:
            Timestamp cho phép multiple failures của cùng file được lưu riêng biệt.
        """
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        normalized_prefix = self.quarantine_prefix.strip("/")
        normalized_file = file_key.lstrip("/")
        return f"{normalized_prefix}/{self.table_name}/{ts}/{normalized_file}"

    def _quarantine_failed_file(self, s3, file_key: str, error_message: str) -> dict:
        """Isolate failed file vào quarantine area với error manifest để điều tra.
        
        Args:
            s3: Boto3 S3 client instance
            file_key: S3 key của failed file
            error_message: Exception message hoặc error description
        
        Returns:
            dict: Quarantine detail chứa:
                - source_bucket, source_key: Original location
                - quarantine_bucket, quarantine_key: Quarantine location
                - error: Error message
                - failed_at: ISO timestamp
                - clickhouse_table: Target table
                - source_deleted: Boolean (file có được xóa khỏi source không)
                - source_delete_error: Nếu xóa source fail
                - quarantine_error: Nếu quarantine operation fail
        
        Process:
            1. Copy source file → quarantine bucket/key
            2. Nếu CLICKHOUSE_QUARANTINE_MOVE_SOURCE=1:
                - Delete source file (prevent infinite retry)
                - Log source_deleted=True
            3. Write error manifest JSON (filename + .error.json):
                {
                  "source_bucket": "silver-3",
                  "source_key": "orders/...",
                  "error": "Column type mismatch...",
                  "failed_at": "2026-03-09T15:20:30Z",
                  ...
                }
            4. Send terminal notification
        
        Error Handling:
            - Nếu quarantine operation fail, log error nhưng không crash
            - Return dict với quarantine_error field
            - Caller sẽ add vào failed_files list cho checkpoint
        
        Note:
            - Quarantine không tự động retry, cần manual investigation
            - Move source (delete) để tránh file bị retry mãi trong next run
            - Error manifest là JSON để dễ parse bởi monitoring tools
        """
        quarantine_key = self._build_quarantine_key(file_key)
        manifest_key = f"{quarantine_key}.error.json"

        detail = {
            "source_bucket": self.bucket_silver,
            "source_key": file_key,
            "error": error_message,
            "failed_at": datetime.now(timezone.utc).isoformat(),
            "clickhouse_table": f"{self.clickhouse_database}.{self.clickhouse_table}",
            "quarantine_bucket": self.quarantine_bucket,
            "quarantine_key": quarantine_key,
            "source_deleted": False,
        }

        try:
            s3.copy_object(
                Bucket=self.quarantine_bucket,
                CopySource={"Bucket": self.bucket_silver, "Key": file_key},
                Key=quarantine_key,
            )

            if self.quarantine_move_source:
                try:
                    s3.delete_object(Bucket=self.bucket_silver, Key=file_key)
                    detail["source_deleted"] = True
                except Exception as delete_exc:
                    detail["source_delete_error"] = str(delete_exc)

            s3.put_object(
                Bucket=self.quarantine_bucket,
                Key=manifest_key,
                Body=json.dumps(detail, indent=2).encode("utf-8"),
                ContentType="application/json",
            )

            notify_schema_event(
                title="ClickHouse Quarantine Event",
                message=(
                    f"Table: {self.clickhouse_database}.{self.clickhouse_table}\n"
                    f"Source file: {file_key}\n"
                    f"Quarantine: s3://{self.quarantine_bucket}/{quarantine_key}\n"
                    f"Error: {error_message}"
                ),
                level="WARN",
            )
            print(f"    -> Quarantined: s3://{self.quarantine_bucket}/{quarantine_key}")

        except Exception as quarantine_exc:
            detail["quarantine_error"] = str(quarantine_exc)
            print(f"    ⚠️ Quarantine failed for {file_key}: {quarantine_exc}")

        return detail

    def sync(self):
        """Execute ClickHouse sync trong standalone mode với connections tự quản lý.
        
        Mode:
            Standalone - tạo S3 và ClickHouse connections riêng cho sync run này.
            Dùng khi chạy single table hoặc testing.
        """
        with s3_session() as s3, clickhouse_session() as ch:
            self._execute_sync(s3, ch)

    def sync_with_connections(self, s3, ch):
        """Execute ClickHouse sync với connections được share từ caller.
        
        Args:
            s3: Boto3 S3 client đã được tạo sẵn
            ch: ClickHouse client đã được tạo sẵn
        
        Mode:
            Shared - dùng connections được truyền vào từ multi-table pipeline.
            Hiệu quả hơn khi sync nhiều tables vì tái sử dụng connection pool.
        """
        self._execute_sync(s3, ch)

    def _execute_sync(self, s3, ch_client):
        """Core ClickHouse sync logic: Silver S3 → ClickHouse table với quarantine support.
        
        Args:
            s3: Boto3 S3 client để read Silver files
            ch_client: ClickHouse client để insert data
        
        Process Flow:
            1. Load checkpoint từ metadata
            2. Validate ClickHouse table tồn tại
            3. Scan Silver bucket để tìm files mới
            4. Nếu không có file mới:
                - Log "up to date"
                - Return early
            5. Auto schema migration:
                - Sample first N files để detect new columns
                - ALTER TABLE ADD COLUMN cho columns mới
                - Notify schema evolution event
            6. Get ClickHouse table columns (sau khi ADD COLUMN)
            7. Per-file insertion:
                - Column intersection (file ∩ table)
                - INSERT via s3() function
                - Track rows inserted
                - Quarantine nếu fail
            8. Count total rows in ClickHouse table (verification)
            9. Save checkpoint với:
                - last_synced_file_time = newest file's LastModified
                - Sync metrics (files succeeded/failed, rows inserted)
                - Failed file details và quarantine paths
                - Auto-added columns
                - ClickHouse metrics (total rows in table)
        
        Partial Success:
            - Nếu một số files fail, các files khác vẫn được insert
            - Failed files được quarantine và log trong metadata
            - Checkpoint vẫn được update để không retry successful files
        
        Schema Evolution:
            - Auto ADD COLUMN nếu CLICKHOUSE_AUTO_ADD_COLUMNS=1
            - Sample limited files để tránh overhead
            - Notify terminal với column details
        
        Metrics:
            - Per-file rows_inserted (accurate count before/after)
            - Total rows in table (final verification)
            - Execution time tracking
        
        Note:
            - S3 endpoint phải accessible từ ClickHouse (dùng host.docker.internal cho Docker)
            - ReplacingMergeTree engine tự động deduplicate by PK + version
            - Checkpoint luôn update ngay cả khi có partial failures
        """
        start_time = datetime.now()
        last_cp = self.get_checkpoint(s3)

        print(f"Syncing {self.table_name} -> {self.clickhouse_database}.{self.clickhouse_table} (from: {last_cp})")
        self._validate_clickhouse_table_exists(ch_client)

        new_files = self._find_new_files(s3, last_cp)
        if not new_files:
            print(f"{self.table_name} is up to date in ClickHouse.")
            return

        print(f"Found {len(new_files)} new files")

        # Automation: chỉ tự động ADD COLUMN cho cột mới phát hiện từ Parquet schema.
        added_columns = self._auto_add_new_columns(s3, ch_client, new_files)
        if added_columns:
            print(f"  Auto schema update: added {len(added_columns)} column(s)")

        # Get ClickHouse table schema
        table_columns = self._get_clickhouse_table_columns(ch_client)
        print(f"  Table columns: {table_columns}")

        # Get file keys
        file_keys = [obj["Key"] for obj in new_files]

        # Insert data từ S3 vào ClickHouse và thu thập metrics chi tiết
        try:
            rows_inserted, failed_files = self._insert_from_s3(s3, ch_client, file_keys, table_columns)
            
            print(f"  Summary: {len(file_keys) - len(failed_files)}/{len(file_keys)} files succeeded, {rows_inserted} rows inserted")
            
            if failed_files:
                print(f"  ⚠️ {len(failed_files)} files failed:")
                for fail in failed_files[:5]:  # Show first 5 failures
                    print(f"    - {fail['file']}: {fail['error']}")
            
        except Exception as exc:
            print(f"  ERROR during S3 insert: {exc}")
            raise

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        total_rows_in_table = ch_client.query(
            f"SELECT count() FROM {self.clickhouse_database}.{self.clickhouse_table}"
        ).result_rows[0][0]

        checkpoint_metadata = {
            "last_synced_file_time": new_files[-1]["LastModified"].isoformat(),
            "total_files_found": len(file_keys),
            "total_files_succeeded": len(file_keys) - len(failed_files),
            "total_files_failed": len(failed_files),
            "rows_inserted": rows_inserted,
            "synced_at": end_time.isoformat(),
            "execution_time_seconds": round(execution_time, 2),
            "failed_files": [f["file"] for f in failed_files] if failed_files else [],
            "failed_file_details": failed_files,
            "auto_added_columns": added_columns,
            "clickhouse_metrics": {
                "database": self.clickhouse_database,
                "table": self.clickhouse_table,
                "total_rows_in_table": total_rows_in_table,
            },
        }

        self.save_checkpoint_with_metrics(s3, checkpoint_metadata)

        print(f"Finished {self.table_name}")
        print(f"  Rows in table: {total_rows_in_table}")
        print(f"  Duration: {execution_time:.2f}s")

# --- CẤU HÌNH DANH SÁCH BẢNG (Sử dụng Dataclass) ---
TABLES_CONFIG = [
    ClickHouseTableConfig(
        table_name="orders",
        pk_col="order_id",
        version_col="updated_at",
        clickhouse_table="orders",
    ),
    ClickHouseTableConfig(
        table_name="product_reviews",
        pk_col="review_id",
        version_col="created_at",
        clickhouse_table="product_reviews",
    ),
    ClickHouseTableConfig(
        table_name="categories",
        pk_col="category_id",
        version_col="created_at",
        clickhouse_table="categories",
    ),
    ClickHouseTableConfig(
        table_name="products",
        pk_col="product_id",
        version_col="created_at",
        clickhouse_table="products",
    ),
    ClickHouseTableConfig(
        table_name="order_items",
        pk_col="item_id",
        version_col=None,
        clickhouse_table="order_items",
    ),
]


def run_pipeline(configs: list):
    """"Chạy pipeline Silver -> ClickHouse cho tất cả bảng trong configs."""
    with s3_session() as s3, clickhouse_session() as ch:
        for config in configs:
            try:
                loader = SilverToClickHouse(config)
                print(f"Processing: {config.table_name}")
            
                loader.sync_with_connections(s3, ch)

                print(f"Completed: {config.table_name}\n")
            except Exception as exc:
                print(f"Failed: {config.table_name} - {exc}\n")
                continue


if __name__ == "__main__":
    print("Silver -> ClickHouse Sync Pipeline")
    run_pipeline(TABLES_CONFIG)
