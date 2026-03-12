import pandas as pd
import os
from datetime import datetime as dt, timezone
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import Optional, Callable

from etl_utils import (
    s3_session,
    get_latest_checkpoint_silver,
    save_checkpoint,
    add_partitions,
    get_partition_paths,
    read_parquet_from_s3,
    write_parquet_to_s3,
    get_latest_layer_metadata,
    diff_schema_columns,
    notify_schema_event,
)
from cleaners import generic_clean, apply_schema_padding, get_cleaner

load_dotenv()

MIN_REQUIRED_SCHEMA = {
    "orders": {
        "order_id": "UInt64",
        "user_id": "UInt64",
        "status": "string",
        "total_amount": "float64",
        "created_at": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
    },
    "product_reviews": {
        "review_id": "UInt64",
        "product_id": "UInt64",
        "user_id": "UInt64",
        "rating": "UInt64",
        "comment": "string",
        "created_at": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
    },
    "categories": {
        "category_id": "UInt64",
        "category_name": "string",
        "description": "string",
        "created_at": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
    },
    "products": {
        "product_id": "UInt64",
        "category_id": "UInt64",
        "product_name": "string",
        "base_price": "float64",
        "stock_quantity": "UInt64",
        "created_at": "datetime64[ns]",
        "updated_at": "datetime64[ns]",
    },
    "order_items": {
        "item_id": "UInt32",
        "order_id": "UInt32",
        "product_id": "UInt32",
        "quantity": "Int32",
        "unit_price": "float64",
        "updated_at": "datetime64[ns]",
    },
    "users": {
        "user_id": "UInt64",
        "updated_at": "datetime64[ns]",
    },
}

# --- DATACLASS CHO TABLE CONFIG (Rõ ràng & Type-safe) ---
@dataclass
class TableConfig:
    """Cấu hình cho mỗi bảng trong pipeline Bronze->Silver.
    
    Attributes:
        table_name: Tên bảng để xử lý (dùng làm prefix cho S3 paths)
        pk_col: Tên cột primary key (để deduplication khi merge data)
        updated_col: Tên cột timestamp (để sort và lấy version mới nhất)
        cleaner_func: Optional custom cleaning function cho table-specific logic
    
    Validation:
        __post_init__ sẽ raise ValueError nếu các trường bắt buộc để trống.
    """
    table_name: str
    pk_col: str
    updated_col: str
    cleaner_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None
    
    def __post_init__(self):
        """Validation sau khi khởi tạo dataclass.
        
        Raises:
            ValueError: Nếu table_name, pk_col, hoặc updated_col để trống
        
        Note:
            __post_init__ là magic method của dataclass, đưức gọi tự động sau __init__.
        """
        if not self.table_name or not self.pk_col or not self.updated_col:
            raise ValueError("table_name, pk_col, updated_col không được để trống")

class SilverIncrementalETL:
    def __init__(self, config: TableConfig):
        """Khởi tạo Silver ETL processor với cấu hình bảng.
        
        Args:
            config: TableConfig instance chứa:
                - table_name: Tên bảng để xử lý
                - pk_col: Primary key column cho deduplication
                - updated_col: Timestamp column cho version tracking
                - cleaner_func: Optional custom cleaning function
        
        Note:
            required_schema được lấy từ MIN_REQUIRED_SCHEMA constant.
            Nếu table không có trong MIN_REQUIRED_SCHEMA, sẽ dùng schema mặc định với pk_col + updated_col.
        """
        self.config = config
        self.table_name = config.table_name
        self.pk_col = config.pk_col
        self.updated_col = config.updated_col
        self.custom_cleaner = config.cleaner_func
        
        self.bucket_bronze = os.getenv("ILOADING_BRONZE_BUCKET_NAME", "bronze-3")
        self.bucket_silver = os.getenv("ILOADING_SILVER_BUCKET_NAME", "silver-3")
        self.initial_start = os.getenv("INITIAL_START", "2026-03-02T00:00:00") 
        self.required_schema = MIN_REQUIRED_SCHEMA.get(
            self.table_name,
            {
                self.pk_col: "UInt64",
                self.updated_col: "datetime64[ns]",
            },
        )

    def get_checkpoint(self, s3):
        """Lấy checkpoint mới nhất từ S3 để xác định file Bronze nào cần xử lý.
        
        Args:
            s3: Boto3 S3 client instance
        
        Returns:
            str: ISO timestamp của last_processed_file_time từ metadata,
                 hoặc INITIAL_START nếu chưa có lịch sử.
        
        Note:
            Checkpoint được lưu trong {table_name}/metadata/silver_checkpoint.json.
        """
        checkpoint = get_latest_checkpoint_silver(s3, self.bucket_silver, self.table_name, self.initial_start)
        return checkpoint

    def save_checkpoint_with_metrics(self, s3, metadata: dict):
        """Lưu checkpoint với data quality metrics lên S3.
        
        Args:
            s3: Boto3 S3 client instance
            metadata: Dictionary chứa:
                - last_processed_file_time: ISO timestamp của file mới nhất đã xử lý
                - total_files_processed: Số files đã process trong run
                - total_records_input: Tổng records từ Bronze
                - total_records_after_clean: Tổng records sau cleaning
                - total_records_written: Tổng records ghi vào Silver
                - total_partitions_written: Số partitions được tạo/update
                - schema_columns: List columns hiện tại
                - data_quality_metrics: Null counts, rows dropped, etc.
                - status: SUCCESS | IN_PROGRESS | UP_TO_DATE
        
        Note:
            Metadata file là overwrite.
            IN_PROGRESS status cho phép resume khi ETL bị interrupt.
        """
        save_checkpoint(s3, self.bucket_silver, self.table_name, metadata, layer='silver')

    def process(self):
        """Execute Silver ETL trong standalone mode với S3 connection tự quản lý.
        
        Mode:
            Standalone - tạo S3 connection riêng cho ETL run này.
            Dùng khi chạy single table hoặc testing.
        """
        with s3_session() as s3:
            self._execute_etl(s3)

    def process_etl_with_connection(self, s3):
        """Execute Silver ETL với S3 connection được share từ caller.
        
        Args:
            s3: Boto3 S3 client đã được tạo sẵn
        
        Mode:
            Shared - dùng connection được truyền vào từ multi-table pipeline.
            Hiệu quả hơn khi chạy nhiều tables vì tái sử dụng connection.
        """
        self._execute_etl(s3)

    @staticmethod
    def _parse_checkpoint_to_timestamp(checkpoint: str) -> float:
        """Parse ISO checkpoint string thành Unix timestamp để so sánh với S3 LastModified.
        
        Args:
            checkpoint: ISO format datetime string (có thể có 'Z' suffix)
        
        Returns:
            float: Unix timestamp (seconds since epoch)
        
        Note:
            - Handle cả aware và naive datetime
            - Naive datetime được normalize thành UTC
            - S3 LastModified trả về aware datetime UTC nên cần normalize để so sánh chính xác
        """
        cp_dt = dt.fromisoformat(checkpoint.replace("Z", "+00:00"))
        if cp_dt.tzinfo is None:
            cp_dt = cp_dt.replace(tzinfo=timezone.utc)
        return cp_dt.timestamp()

    @staticmethod
    def _accumulate_metric_counts(target: dict, source: dict):
        """Cộng dồn metrics từ nhiều files vào aggregate dictionary.
        
        Args:
            target: Dictionary đích chứa accumulated metrics (modified in-place)
            source: Dictionary nguồn chứa metrics từ file hiện tại
        
        Behavior:
            - Nếu key chưa tồn tại trong target, khởi tạo = 0
            - Cộng dồn value từ source vào target (cast to int)
        """
        for k, v in source.items():
            target[k] = int(target.get(k, 0)) + int(v)

    def _execute_etl(self, s3):
        """Core Silver ETL logic: Stream Bronze files → Clean → Deduplicate → Merge to Silver.
        
        Args:
            s3: Boto3 S3 client để read Bronze và write Silver
        
        Process Flow:
            1. Load checkpoint và schema từ run trước
            2. Scan Bronze bucket để tìm files có LastModified > checkpoint
            3. Nếu không có file mới:
                - Save heartbeat metadata với status UP_TO_DATE
                - Return early
            4. STREAMING PROCESSING (không concat tất cả files):
                For each Bronze file:
                    a. Read parquet file
                    b. Collect null counts BEFORE cleaning
                    c. Apply generic_clean (drop null PK, parse timestamps)
                    d. Apply custom cleaner nếu có
                    e. Apply schema_padding để đảm bảo MIN_REQUIRED_SCHEMA
                    f. Partition theo year/month/day
                    g. For each partition:
                        - Read existing Silver partition (nếu có)
                        - Schema alignment (reindex columns)
                        - Concat old + new data
                        - Deduplicate by PK (keep latest by updated_col)
                        - Drop partition columns (y, m, d)
                        - Write back to Silver
                    h. Save IN_PROGRESS checkpoint sau mỗi file (resume support)
            5. Schema evolution detection:
                - Compare current_columns vs previous_columns
                - Notify terminal nếu có added/removed columns
            6. Update final checkpoint với:
                - last_processed_file_time = newest file's LastModified
                - Total metrics aggregated từ tất cả files
                - Data quality metrics (null counts, rows dropped)
                - Schema columns và evolution info
                - Status = SUCCESS
        
        Streaming: Process từng file → tiết kiệm memory + resume support
        
        Resumability:
            - Checkpoint được update sau mỗi file với status IN_PROGRESS
            - Nếu run bị interrupt, run tiếp theo sẽ skip files đã process
        
        Data Quality:
            - null_counts_before_clean: Null distribution từ Bronze
            - null_counts_after_clean: Null distribution sau cleaning
            - rows_dropped_during_clean: Số records bị drop (null PK, invalid data)
        """
        last_cp = self.get_checkpoint(s3)
        start_time = dt.now()
        print(f"🚀 Processing Silver: {self.table_name} (From: {last_cp})")

        # Load previous metadata để so sánh schema và hỗ trợ checkpointing
        previous_meta = get_latest_layer_metadata(s3, self.bucket_silver, self.table_name, layer='silver')
        previous_columns = previous_meta.get('schema_columns', [])

        try:
            last_cp_ts = self._parse_checkpoint_to_timestamp(last_cp)
        except ValueError:
            print(f"⚠️ Invalid checkpoint format '{last_cp}'. Fallback to INITIAL_START.")
            last_cp_ts = self._parse_checkpoint_to_timestamp(self.initial_start)

        # 1. Tìm file Bronze mới
        paginator = s3.get_paginator('list_objects_v2')
        new_files = []
        for page in paginator.paginate(Bucket=self.bucket_bronze, Prefix=f"{self.table_name}/"):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet') and obj['LastModified'].timestamp() > last_cp_ts:
                        new_files.append(obj)

        if not new_files:
            heartbeat_metadata = {
                "last_processed_file_time": previous_meta.get("last_processed_file_time", last_cp),
                "total_files_processed": 0,
                "total_records_input": 0,
                "total_records_after_clean": 0,
                "total_records_written": 0,
                "total_partitions_written": 0,
                "processed_at": dt.now().isoformat(),
                "execution_time_seconds": round((dt.now() - start_time).total_seconds(), 2),
                "schema_columns": previous_columns or sorted(self.required_schema.keys()),
                "schema_evolution": {
                    "changed": False,
                    "added_columns": [],
                    "removed_columns": [],
                },
                "required_schema": self.required_schema,
                "status": "UP_TO_DATE",
                "data_quality_metrics": {
                    "rows_dropped_during_clean": 0,
                    "null_counts_before_clean": {},
                    "null_counts_after_clean": {},
                    "has_custom_cleaner": self.custom_cleaner is not None,
                },
            }
            self.save_checkpoint_with_metrics(s3, heartbeat_metadata)
            print(f"✅ {self.table_name} is up to date.")
            return

        new_files = sorted(new_files, key=lambda x: x['LastModified'])

        # 2. Stream processing theo từng file Bronze để giảm memory và dễ resume khi fail.
        total_rows_input = 0
        total_rows_after_clean = 0
        total_records_written = 0
        touched_partitions = set()
        null_counts_before = {}
        null_counts_after = {}
        run_schema_columns = set(previous_columns)

        # Thông báo tiến độ processing từng file để dễ theo dõi khi chạy nhiều files.
        for idx, file_obj in enumerate(new_files, start=1):
            file_key = file_obj['Key']
            print(f"  -> Streaming file {idx}/{len(new_files)}: {file_key}")

            # Read file từ Bronze vào DataFrame
            df_new = read_parquet_from_s3(s3, self.bucket_bronze, file_key)
            file_rows_input = len(df_new)
            total_rows_input += file_rows_input
            self._accumulate_metric_counts(null_counts_before, df_new.isnull().sum().to_dict())

            # Clean data với generic_clean + custom cleaner (nếu có)
            df_new = generic_clean(
                df_new,
                self.pk_col,
                self.updated_col,
                required_schema=self.required_schema,
            )

            if self.custom_cleaner:
                df_new = self.custom_cleaner(df_new)

            # Apply schema padding để đảm bảo có đủ columns cho downstream processing (partitioning, merging)
            df_new = apply_schema_padding(df_new, self.required_schema)
            run_schema_columns.update([str(col) for col in df_new.columns.tolist()])

            file_rows_after_clean = len(df_new)
            total_rows_after_clean += file_rows_after_clean
            self._accumulate_metric_counts(null_counts_after, df_new.isnull().sum().to_dict())

            # Nếu sau khi clean mà không còn record nào, skip bước merge để tiết kiệm I/O.
            if file_rows_after_clean > 0:
                df_new = add_partitions(df_new, self.updated_col)

                # Merge từng partition vào Silver với logic deduplication theo PK + updated_col
                for p_path, df_part_new in get_partition_paths(df_new, self.table_name, suffix="/clean_data.parquet"):
                    df_part_new = apply_schema_padding(df_part_new, self.required_schema)

                    try:
                        # Nếu partition đã tồn tại, đọc data cũ để merge. Nếu không, tạo mới với data mới.
                        df_old = read_parquet_from_s3(s3, self.bucket_silver, p_path)
                        df_old = apply_schema_padding(df_old, self.required_schema)

                        combined_cols = list(dict.fromkeys(df_old.columns.tolist() + df_part_new.columns.tolist()))
                        df_old = df_old.reindex(columns=combined_cols)
                        df_part_new = df_part_new.reindex(columns=combined_cols)
                        df_combined = pd.concat([df_old, df_part_new], ignore_index=True)
                    except Exception:
                        df_combined = df_part_new

                    df_final = df_combined.sort_values(self.updated_col, ascending=False).drop_duplicates(subset=[self.pk_col])
                    df_final_clean = df_final.drop(columns=['y', 'm', 'd'], errors='ignore')

                    write_parquet_to_s3(s3, self.bucket_silver, p_path, df_final_clean)
                    touched_partitions.add(p_path)
                    total_records_written += len(df_part_new)

            # Checkpoint tiến độ sau mỗi file đã xử lý xong để hỗ trợ resume.
            progress_columns = sorted(run_schema_columns) if run_schema_columns else sorted(self.required_schema.keys())
            progress_metadata = {
                "last_processed_file_time": file_obj['LastModified'].isoformat(),
                "total_files_processed": idx,
                "total_records_input": total_rows_input,
                "total_records_after_clean": total_rows_after_clean,
                "total_records_written": total_records_written,
                "total_partitions_written": len(touched_partitions),
                "processed_at": dt.now().isoformat(),
                "execution_time_seconds": round((dt.now() - start_time).total_seconds(), 2),
                "schema_columns": progress_columns,
                "schema_evolution": {
                    "changed": False,
                    "added_columns": [],
                    "removed_columns": [],
                },
                "required_schema": self.required_schema,
                "status": "IN_PROGRESS",
                "data_quality_metrics": {
                    "rows_dropped_during_clean": total_rows_input - total_rows_after_clean,
                    "null_counts_before_clean": {k: v for k, v in null_counts_before.items() if v > 0},
                    "null_counts_after_clean": {k: v for k, v in null_counts_after.items() if v > 0},
                    "has_custom_cleaner": self.custom_cleaner is not None,
                },
            }
            self.save_checkpoint_with_metrics(s3, progress_metadata)

        # 3. Sau khi xử lý tất cả files, so sánh schema columns để detect evolution.
        current_columns = sorted(run_schema_columns) if run_schema_columns else sorted(self.required_schema.keys())
        schema_diff = diff_schema_columns(previous_columns, current_columns)
        if previous_columns and schema_diff['changed']:
            msg = (
                f"Table: {self.table_name}\n"
                f"Added columns: {schema_diff['added_columns'] or '[]'}\n"
                f"Removed columns: {schema_diff['removed_columns'] or '[]'}\n"
                f"Layer: Silver"
            )
            notify_schema_event(title="Schema Evolution Alert - Silver", message=msg, level="WARN")
            print(f"⚠️ Schema changed for {self.table_name}: +{schema_diff['added_columns']} -{schema_diff['removed_columns']}")

        partition_count = len(touched_partitions)
        rows_dropped = total_rows_input - total_rows_after_clean

        # 4. Update Checkpoint với metadata toàn diện
        end_time = dt.now()
        execution_time = (end_time - start_time).total_seconds()
        
        checkpoint_metadata = {
            "last_processed_file_time": new_files[-1]['LastModified'].isoformat(),
            "total_files_processed": len(new_files),
            "total_records_input": total_rows_input,
            "total_records_after_clean": total_rows_after_clean,
            "total_records_written": total_records_written,
            "total_partitions_written": partition_count,
            "processed_at": end_time.isoformat(),
            "execution_time_seconds": round(execution_time, 2),
            "schema_columns": schema_diff["current_columns"],
            "schema_evolution": {
                "changed": schema_diff["changed"],
                "added_columns": schema_diff["added_columns"],
                "removed_columns": schema_diff["removed_columns"],
            },
            "required_schema": self.required_schema,
            "status": "SUCCESS",
            "data_quality_metrics": {
                "rows_dropped_during_clean": rows_dropped,
                "null_counts_before_clean": {k: int(v) for k, v in null_counts_before.items() if v > 0},
                "null_counts_after_clean": {k: int(v) for k, v in null_counts_after.items() if v > 0},
                "has_custom_cleaner": self.custom_cleaner is not None
            }
        }
        
        self.save_checkpoint_with_metrics(s3, checkpoint_metadata)
        print(f"🏁 Finished {self.table_name} | Records: {total_records_written} | Partitions: {partition_count} | Time: {execution_time:.2f}s")

# --- CẤU HÌNH DANH SÁCH BẢNG (Sử dụng Dataclass) ---
TABLES_CONFIG = [
    TableConfig(
        table_name="orders",
        pk_col="order_id",
        updated_col="updated_at",
        cleaner_func=get_cleaner("orders")
    ),
    TableConfig(
        table_name="users",
        pk_col="user_id",
        updated_col="updated_at",
        cleaner_func=get_cleaner("users")
    ),
    TableConfig(
        table_name="categories",
        pk_col="category_id",
        updated_col="updated_at"
    ),
    TableConfig(
        table_name="products",
        pk_col="product_id",
        updated_col="updated_at"
    ),
    TableConfig(
        table_name="product_reviews",
        pk_col="review_id",
        updated_col="updated_at"
    ),
    TableConfig(
        table_name="order_items",
        pk_col="item_id",
        updated_col="updated_at"
    )
]

def run_pipeline(configs: list):
    """Chạy ETL cho nhiều bảng, dùng chung 1 S3 connection"""
    with s3_session() as s3:
        for config in configs:
            try:
                etl = SilverIncrementalETL(config)
                print(f"Processing: {config.table_name}")

                etl.process_etl_with_connection(s3)

                print(f"Completed: {config.table_name}\n")
            except Exception as e:
                print(f"Failed: {config.table_name} - {e}\n")
                continue


if __name__ == "__main__":
    run_pipeline(TABLES_CONFIG)