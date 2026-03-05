import pandas as pd
import json
import os
from datetime import datetime, timedelta
from io import BytesIO
from dataclasses import dataclass
from typing import Optional

from etl_utils import db_session, s3_session, BaseTableConfig, get_latest_checkpoint_bronze, save_checkpoint, add_partitions, get_partition_paths, write_parquet_to_s3

from dotenv import load_dotenv

load_dotenv(override=True)

# --- DATACLASS CHO TABLE CONFIG (Extend BaseTableConfig) ---
@dataclass
class OLTPTableConfig(BaseTableConfig):
    """Cấu hình cho mỗi bảng trong pipeline OLTP->Bronze"""
    pass

class GenericETL:
    def __init__(self, config: OLTPTableConfig):
        """
        Init với OLTPTableConfig dataclass thay vì tham số riêng lẻ
        
        Args:
            config: OLTPTableConfig instance chứa cấu hình bảng
        """
        self.config = config
        self.table_name = config.table_name
        self.pk_col = config.pk_col
        self.updated_col = config.updated_col
        
        # Cấu hình kết nối
        self.bucket = os.getenv("ILOADING_BRONZE_BUCKET_NAME")
        self.window_min = int(os.getenv("WINDOW_MINUTES"))
        self.delta_min = int(os.getenv("DELTA_MINUTES"))
        self.initial_start = os.getenv("INITIAL_START")

    def get_latest_checkpoint(self, s3):
        """Lấy checkpoint mới nhất từ S3, nếu không có thì dùng giá trị mặc định"""
        checkpoint = get_latest_checkpoint_bronze(s3, self.bucket, self.table_name, self.initial_start)
        print(f"Loaded checkpoint: {checkpoint}")
        return checkpoint

    def get_fingerprints_in_range(self, s3, start_dt, end_dt):
        """Lấy tất cả fingerprint (PK + UpdatedAt) trong vùng overlap để loại bỏ những bản ghi trùng lặp"""
        existing_fps = set()
        date_range = pd.date_range(start=start_dt.date(), end=end_dt.date())
        
        # Chuyển start_dt về dạng số để so sánh (ví dụ: 024151)
        start_time_int = int(start_dt.strftime('%H%M%S'))
        
        for dt in date_range:
            # Lấy tất cả file trong ngày đó dựa trên partitioning path
            prefix = f"{self.table_name}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            paginator = s3.get_paginator('list_objects_v2')
            
            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_key = obj['Key']
                        if not file_key.endswith('.parquet'):
                            continue
                        
                        # Trích xuất timestamp từ tên file (data_HHMMSS.parquet)
                        try:
                            file_name = file_key.split('/')[-1]
                            file_time_str = file_name.replace('data_', '').replace('.parquet', '')
                            file_time_int = int(file_time_str)
                            
                            # LOGIC LỌC FILE:
                            # Nếu ngày của file là ngày start_dt, nhưng upper_bound của file < start_time_int
                            # thì file đó chắc chắn không chứa dữ liệu trùng.
                            if dt.date() == start_dt.date() and file_time_int < start_time_int:
                                continue
                                
                        except ValueError:
                            # Nếu tên file không đúng định dạng, bỏ qua lọc và đọc luôn cho an toàn
                            pass

                        # Chỉ đọc file thỏa mãn điều kiện để lấy fingerprint
                        print(f"Reading file for dedup: {file_key}")
                        resp = s3.get_object(Bucket=self.bucket, Key=file_key)
                        df_tmp = pd.read_parquet(BytesIO(resp['Body'].read()), columns=[self.pk_col, self.updated_col])
                        
                        # Chuyển cột updated_at sang datetime để so khớp chính xác
                        df_tmp[self.updated_col] = pd.to_datetime(df_tmp[self.updated_col])
                        
                        # (Tùy chọn) Lọc bớt trong DataFrame chỉ lấy những dòng thực sự nằm trong start_dt
                        df_tmp = df_tmp[df_tmp[self.updated_col] >= start_dt]
                        
                        fps = list(zip(df_tmp[self.pk_col], df_tmp[self.updated_col]))
                        existing_fps.update(fps)
                        
        return existing_fps

    def run(self):
        """Main ETL Logic - tạo connections riêng (Standalone mode)"""
        with db_session() as engine, s3_session() as s3:
            self._execute_etl(engine, s3)

    def run_etl_with_connections(self, engine, s3):
        """Main ETL Logic - sử dụng connections đã được cung cấp (Shared mode)"""
        self._execute_etl(engine, s3)

    def _execute_etl(self, engine, s3):
        """Phần core ETL logic"""
        start_time = datetime.now()
        last_checkpoint = datetime.fromisoformat(self.get_latest_checkpoint(s3))
        
        # Tính toán Window
        lower_bound = last_checkpoint - timedelta(minutes=self.delta_min)
        upper_bound = lower_bound + timedelta(minutes=self.window_min)
        if upper_bound > start_time: upper_bound = start_time - timedelta(seconds=60)
        
        print(f"Processing [{self.table_name}]: {lower_bound} -> {upper_bound}")

        # Extract dữ liệu mới từ OLTP dựa trên cột updated_at
        query = f"SELECT * FROM {self.table_name} WHERE {self.updated_col} > '{lower_bound}' AND {self.updated_col} <= '{upper_bound}'"
        df_raw = pd.read_sql(query, engine)
        
        oltp_count = len(df_raw)
        dup_count = 0
        status = "SUCCESS"

        # Nếu có dữ liệu mới, tiến hành loại bỏ trùng lặp và load lên S3
        if not df_raw.empty:
            df_raw[self.updated_col] = pd.to_datetime(df_raw[self.updated_col])
            
            # Lấy tất cả fingerprint (PK + UpdatedAt) trong vùng overlap để loại bỏ những bản ghi trùng lặp
            existing_fps = self.get_fingerprints_in_range(s3, lower_bound, upper_bound)
            df_raw['fp'] = list(zip(df_raw[self.pk_col], df_raw[self.updated_col]))
            df_diff = df_raw[~df_raw['fp'].isin(existing_fps)].copy()
            df_diff.drop(columns=['fp'], inplace=True)
            
            dup_count = oltp_count - len(df_diff)

            # Nếu có dữ liệu mới sau khi loại bỏ trùng lặp, tiến hành partitioning và load lên S3
            if not df_diff.empty:
                # Partitioning theo ngày dựa trên cột updated_at
                df_diff = add_partitions(df_diff, self.updated_col)
                
                for p_path, group in get_partition_paths(df_diff, self.table_name, suffix=f"/data_{upper_bound.strftime('%H%M%S')}.parquet"):
                    write_parquet_to_s3(s3, self.bucket, p_path, group)
                
                # Cập nhật checkpoint mới nhất dựa trên cột updated_at lớn nhất trong batch hiện tại
                final_checkpoint = df_raw[self.updated_col].max().isoformat()
            else:
                # Nếu không có dữ liệu mới nào sau khi loại bỏ trùng lặp, checkpoint sẽ được đẩy lên theo window để tránh bị kẹt ở cùng một điểm
                status = "NO_CHANGES"
                final_checkpoint = (last_checkpoint + timedelta(minutes=self.window_min-1)).isoformat()
                if datetime.fromisoformat(final_checkpoint) > start_time: 
                    final_checkpoint = (start_time - timedelta(seconds=60)).isoformat()
        else:
            # Nếu không có dữ liệu nào được trả về từ OLTP, checkpoint sẽ được đẩy lên theo window để tránh bị kẹt ở cùng một điểm
            status = "EMPTY_SOURCE"
            final_checkpoint = (last_checkpoint + timedelta(minutes=self.window_min-1)).isoformat()
            if datetime.fromisoformat(final_checkpoint) > start_time: 
                final_checkpoint = (start_time - timedelta(seconds=60)).isoformat()

        # Metadata
        integrity_metrics = {
            "start_time": lower_bound.isoformat(),
            "end_time": upper_bound.isoformat(),
            "oltp_total_rows": oltp_count,
            "duplicate_rows_removed": dup_count,
            "delta_upserted": (oltp_count - dup_count),
            "status": status
        }
        self.save_audit(s3, final_checkpoint, integrity_metrics)

    def save_audit(self, s3, checkpoint, integrity_metrics):
        """Lưu metadata audit lên S3 để theo dõi và làm checkpoint cho lần chạy tiếp theo"""
        metadata_data = {
            "last_updated_at": checkpoint,
            "etl_performance": {
                "start_time": integrity_metrics.get("start_time"),
                "end_time": integrity_metrics.get("end_time"),
            },
            "integrity_check": {
                "oltp_total_rows": integrity_metrics.get("oltp_total_rows", 0),
                "duplicate_rows_removed": integrity_metrics.get("duplicate_rows_removed", 0),
                "delta_upserted_total": integrity_metrics.get("delta_upserted", 0),
            },
            "status": integrity_metrics.get("status", "SUCCESS")
        }
        
        save_checkpoint(s3, self.bucket, self.table_name, metadata_data, layer='bronze')
        print(f"🏁 {integrity_metrics.get('status')} | Written: {integrity_metrics.get('delta_upserted')} | CP: {checkpoint}")

# --- CẤU HÌNH DANH SÁCH BẢNG (Sử dụng Dataclass) ---
TABLES_TO_SYNC = [
    OLTPTableConfig(table_name="orders", pk_col="order_id", updated_col="updated_at"),
    OLTPTableConfig(table_name="users", pk_col="user_id", updated_col="updated_at"),
    OLTPTableConfig(table_name="products", pk_col="product_id", updated_col="updated_at")
]

def run_pipeline(configs: list):
    """
    Chạy ETL cho nhiều bảng, REUSE connections để tối ưu hiệu suất.
    
    Args:
        configs: Danh sách OLTPTableConfig
    """
    with db_session() as engine, s3_session() as s3:
        for config in configs:
            try:
                etl = GenericETL(config)
                print(f"\n{'='*60}")
                print(f"Processing: {config.table_name}")
                print(f"{'='*60}")
                
                # Gọi run_etl() thay vì run() để pass connections
                etl.run_etl_with_connections(engine, s3)
                
                print(f"Completed: {config.table_name}\n")
            except Exception as e:
                print(f"Failed: {config.table_name} - {e}\n")
                continue


if __name__ == "__main__":
    """Chạy ETL cho tất cả bảng với shared connections"""
    run_pipeline(TABLES_TO_SYNC)