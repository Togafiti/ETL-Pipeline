import pandas as pd
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from dataclasses import dataclass
from typing import Optional, Callable

from etl_utils import s3_session, get_latest_checkpoint_silver, save_checkpoint, add_partitions, get_partition_paths, read_parquet_from_s3, write_parquet_to_s3
from cleaners import generic_clean, get_cleaner

load_dotenv()

# --- DATACLASS CHO TABLE CONFIG (Rõ ràng & Type-safe) ---
@dataclass
class TableConfig:
    """Cấu hình cho mỗi bảng trong pipeline Silver"""
    table_name: str
    pk_col: str
    updated_col: str
    cleaner_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None
    
    def __post_init__(self):
        """Validation sau init"""
        if not self.table_name or not self.pk_col or not self.updated_col:
            raise ValueError("table_name, pk_col, updated_col không được để trống")

class SilverIncrementalETL:
    def __init__(self, config: TableConfig):
        """
        Init với TableConfig dataclass để đảm bảo cấu hình rõ ràng và type-safe
        """
        self.config = config
        self.table_name = config.table_name
        self.pk_col = config.pk_col
        self.updated_col = config.updated_col
        self.custom_cleaner = config.cleaner_func
        
        self.bucket_bronze = os.getenv("ILOADING_BRONZE_BUCKET_NAME", "bronze-3")
        self.bucket_silver = os.getenv("ILOADING_SILVER_BUCKET_NAME", "silver-3")
        self.initial_start = os.getenv("INITIAL_START", "2026-03-02T00:00:00") 

    def get_checkpoint(self, s3):
        """Lấy checkpoint từ S3"""
        checkpoint = get_latest_checkpoint_silver(s3, self.bucket_silver, self.table_name, self.initial_start)
        return checkpoint

    def save_checkpoint_with_metrics(self, s3, metadata: dict):
        """Lưu checkpoint với metadata toàn diện lên S3"""
        save_checkpoint(s3, self.bucket_silver, self.table_name, metadata, layer='silver')

    def process(self):
        """Main ETL logic - tạo S3 connection riêng (standalone mode)"""
        with s3_session() as s3:
            self._execute_etl(s3)

    def process_etl_with_connection(self, s3):
        """Main ETL logic - dùng S3 connection có sẵn (shared mode)"""
        self._execute_etl(s3)

    @staticmethod
    def _parse_checkpoint_to_timestamp(checkpoint: str) -> float:
        """Parse ISO checkpoint string thành timestamp để so sánh ổn định."""
        cp_dt = datetime.fromisoformat(checkpoint.replace("Z", "+00:00"))
        if cp_dt.tzinfo is None:
            cp_dt = cp_dt.replace(tzinfo=timezone.utc)
        return cp_dt.timestamp()

    def _execute_etl(self, s3):
        """Phần core ETL logic"""
        from datetime import datetime as dt
        
        last_cp = self.get_checkpoint(s3)
        start_time = dt.now()
        print(f"🚀 Processing Silver: {self.table_name} (From: {last_cp})")

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
            print(f"✅ {self.table_name} is up to date.")
            return

        new_files = sorted(new_files, key=lambda x: x['LastModified'])

        # 2. Đọc & Chuẩn hóa dữ liệu mới
        dfs = [read_parquet_from_s3(s3, self.bucket_bronze, f['Key']) for f in new_files]
        df_new = pd.concat(dfs, ignore_index=True)
        total_rows_input = len(df_new)
        
        # Data quality tracking - trước khi clean
        null_counts_before = df_new.isnull().sum().to_dict()

        # Gọi hàm chuẩn hóa chung
        df_new = generic_clean(df_new, self.pk_col, self.updated_col)

        # Gọi hàm chuẩn hóa riêng (nếu có)
        if self.custom_cleaner:
            df_new = self.custom_cleaner(df_new)
        
        # Data quality tracking - sau khi clean
        total_rows_after_clean = len(df_new)
        rows_dropped = total_rows_input - total_rows_after_clean
        null_counts_after = df_new.isnull().sum().to_dict()

        # 3. Merge vào Silver theo Partition
        df_new = add_partitions(df_new, self.updated_col)
        partition_count = 0
        total_records_written = 0

        for p_path, df_part_new in get_partition_paths(df_new, self.table_name, suffix="/clean_data.parquet"):
            try:
                df_old = read_parquet_from_s3(s3, self.bucket_silver, p_path)
                df_old[self.updated_col] = pd.to_datetime(df_old[self.updated_col])
                df_combined = pd.concat([df_old, df_part_new], ignore_index=True)
            except Exception:
                df_combined = df_part_new

            # Deduplicate tuyệt đối
            df_final = df_combined.sort_values(self.updated_col, ascending=False).drop_duplicates(subset=[self.pk_col])
            df_final_clean = df_final.drop(columns=['y', 'm', 'd'], errors='ignore')

            write_parquet_to_s3(s3, self.bucket_silver, p_path, df_final_clean)
            partition_count += 1
            total_records_written += len(df_final_clean)

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