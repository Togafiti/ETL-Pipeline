import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional, List

from dotenv import load_dotenv

from etl_utils import (
    clickhouse_session,
    get_latest_checkpoint_clickhouse,
    s3_session,
    save_checkpoint,
)

load_dotenv()

def _parse_iso_datetime(value: str) -> datetime:
    """Parse ISO string and handle trailing Z timezone format."""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    # Normalize naive timestamps as UTC to avoid aware-vs-naive comparison errors.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass
class ClickHouseTableConfig:
    """Config cho mỗi bảng trong pipeline Silver->ClickHouse."""

    table_name: str
    pk_col: str
    version_col: Optional[str] = None
    clickhouse_table: Optional[str] = None
    clickhouse_database: str = "analytics"

    def __post_init__(self):
        if not self.table_name or not self.pk_col:
            raise ValueError("table_name va pk_col khong duoc de trong")

        if not self.clickhouse_table:
            self.clickhouse_table = self.table_name


class SilverToClickHouse:
    """Load data từ Silver S3 bucket đến ClickHouse sử dụng S3 table function."""
    
    def __init__(self, config: ClickHouseTableConfig):
        self.config = config
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

        if not self.bucket_silver:
            raise ValueError("ILOADING_SILVER_BUCKET_NAME environment variable not set")

    def get_checkpoint(self, s3) -> str:
        """Lấy checkpoint của layer ClickHouse theo từng table."""
        return get_latest_checkpoint_clickhouse(
            s3,
            self.bucket_silver,
            self.table_name,
            self.initial_start,
        )

    def save_checkpoint_with_metrics(self, s3, metadata: dict):
        """Lưu metadata sync cho ClickHouse layer."""
        save_checkpoint(
            s3,
            self.bucket_silver,
            self.table_name,
            metadata,
            layer="clickhouse",
        )

    def _validate_clickhouse_table_exists(self, ch_client):
        """Kiểm tra table ClickHouse đã tồn tại."""
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
        """Lấy danh sách column của ClickHouse table (trừ các internal columns)."""
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

    def _find_new_files(self, s3, last_cp: str) -> List[dict]:
        """Tìm các file mới hơn checkpoint."""
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

    def _insert_from_s3(self, ch_client, file_keys: List[str], table_columns: List[str]):
        """Insert data từ S3 vào ClickHouse bằng S3 table function."""
        if not file_keys:
            return 0, []
        
        # Build columns SELECT list
        columns_select = ", ".join(table_columns)
        columns_insert = ", ".join(table_columns)
        
        total_rows_inserted = 0
        failed_files = []
        
        # Insert từng file một để dễ dàng tracking lỗi và tính toán số rows inserted chính xác
        for file_key in file_keys:
            try:
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
                
                print(f"    ✓ {file_key}: +{rows_added} rows")
                
            except Exception as exc:
                print(f"    ✗ {file_key}: FAILED - {exc}")
                failed_files.append({"file": file_key, "error": str(exc)})
                continue
        
        return total_rows_inserted, failed_files

    def sync(self):
        """Standalone mode."""
        with s3_session() as s3, clickhouse_session() as ch:
            self._execute_sync(s3, ch)

    def sync_with_connections(self, s3, ch):
        """Shared connection mode."""
        self._execute_sync(s3, ch)

    def _execute_sync(self, s3, ch_client):
        """Core logic để sync từ Silver S3 đến ClickHouse, bao gồm checkpointing và metrics."""
        start_time = datetime.now()
        last_cp = self.get_checkpoint(s3)

        print(f"Syncing {self.table_name} -> {self.clickhouse_database}.{self.clickhouse_table} (from: {last_cp})")
        self._validate_clickhouse_table_exists(ch_client)

        new_files = self._find_new_files(s3, last_cp)
        if not new_files:
            print(f"{self.table_name} is up to date in ClickHouse.")
            return

        print(f"Found {len(new_files)} new files")

        # Get ClickHouse table schema
        table_columns = self._get_clickhouse_table_columns(ch_client)
        print(f"  Table columns: {table_columns}")

        # Get file keys
        file_keys = [obj["Key"] for obj in new_files]

        # Insert data từ S3 vào ClickHouse và thu thập metrics chi tiết
        try:
            rows_inserted, failed_files = self._insert_from_s3(ch_client, file_keys, table_columns)
            
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
