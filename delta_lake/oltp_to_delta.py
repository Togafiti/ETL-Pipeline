import pandas as pd
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

class DeltaETL:
    def __init__(self, table_name, primary_key="id"):
        self.table_name = table_name
        self.primary_key = primary_key
        
        # Cấu hình MinIO
        self.storage_options = {
            "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
            "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
            "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL"),
            "AWS_REGION": os.getenv("AWS_REGION"),
            "AWS_S3_ALLOW_UNSAFE_RENAME": os.getenv("AWS_S3_ALLOW_UNSAFE_RENAME"),
            "AWS_ALLOW_HTTP": os.getenv("AWS_ALLOW_HTTP"),
            "AWS_STORAGE_ALLOW_HTTP": os.getenv("AWS_ALLOW_HTTP")
        }
        
        # Cấu hình S3 bucket và đường dẫn Delta Lake
        self.bucket = os.getenv("DELTALAKE_BUCKET_NAME")
        self.delta_path = f"s3://{self.bucket}/{self.table_name}_delta"
        self.s3_prefix = f"{self.table_name}_delta/metadata/checkpoint_"
        
        # Kết nối DB
        self.engine = create_engine(
            f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
        )
        
        # Kết nối S3
        self.s3 = boto3.client("s3", endpoint_url=self.storage_options["AWS_ENDPOINT_URL"],
                               aws_access_key_id=self.storage_options["AWS_ACCESS_KEY_ID"],
                               aws_secret_access_key=self.storage_options["AWS_SECRET_ACCESS_KEY"])

    # Lấy checkpoint mới nhất từ S3, nếu không có thì dùng giá trị mặc định
    def get_latest_checkpoint(self):
        try:    
            # Lấy file metadata mới nhất để xác định checkpoint
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=self.s3_prefix)
            if 'Contents' in response:
                # Sắp xếp lấy file metadata mới nhất dựa trên LastModified
                latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
                obj = self.s3.get_object(Bucket=self.bucket, Key=latest_file)
                return json.loads(obj['Body'].read().decode('utf-8'))['last_updated_at']
        except Exception:
            pass
        return os.getenv("INITIAL_START")

    # Lưu metadata audit lên S3 để theo dõi và làm checkpoint cho lần chạy tiếp theo
    def save_metadata(self, timestamp_str, metrics):
        """Lưu file audit/checkpoint JSON"""
        file_time_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = f"{self.table_name}_delta/metadata/checkpoint_{file_time_str}.json"
        
        checkpoint_data = {
            "table_name": self.table_name,
            "last_updated_at": timestamp_str,
            "etl_performance": {
                "start_time": metrics['start_time'],
                "duration_seconds": metrics['duration_seconds']
            },
            "integrity_check": metrics,
            "status": metrics['status']
        }

        self.s3.put_object(
            Bucket=self.bucket, 
            Key=path, 
            Body=json.dumps(checkpoint_data, indent=4)
        )
        print(f"✅ [{self.table_name}] Audit Log: {path}")

    # Chạy ETL
    def run(self):
        start_time = datetime.now()
        last_checkpoint_str = self.get_latest_checkpoint()
        last_partition = datetime.fromisoformat(last_checkpoint_str)
        
        # Tính toán Window
        win_min = int(os.getenv("WINDOW_MINUTES"))
        delta_min = int(os.getenv("DELTA_MINUTES"))
        
        lower_bound = last_partition - timedelta(minutes=delta_min)
        potential_upper = lower_bound + timedelta(minutes=win_min)
        now = datetime.now()
        upper_bound = potential_upper if potential_upper < now else (now - timedelta(seconds=60))

        print(f"🚀 Table: {self.table_name} | Range: {lower_bound} to {upper_bound}")

        # Extract
        query = f"SELECT * FROM {self.table_name} WHERE updated_at > '{lower_bound}' AND updated_at <= '{upper_bound}'"
        df_raw = pd.read_sql(query, self.engine)
        
        oltp_count = len(df_raw)
        dup_removed = 0
        rows_ins, rows_upd = 0, 0
        status = "SUCCESS"

        # Nếu có dữ liệu mới từ OLTP
        if not df_raw.empty:
            df_raw['updated_at'] = pd.to_datetime(df_raw['updated_at'])
            
            # Dedup (So khớp với Delta Lake hiện tại)
            try:
                dt_existing = DeltaTable(self.delta_path, storage_options=self.storage_options)
                df_current = dt_existing.to_pandas(columns=[self.primary_key, 'updated_at'])
                df_current['updated_at'] = pd.to_datetime(df_current['updated_at'])
                
                # So sánh để tìm các bản ghi mới hoặc đã cập nhật
                merged = df_raw.merge(df_current, on=[self.primary_key, 'updated_at'], how='left', indicator=True)
                df_new = merged[merged['_merge'] == 'left_only'].drop(columns=['_merge'])
                dup_removed = oltp_count - len(df_new)
            except:
                df_new = df_raw
                print("New table initialization...")

            if not df_new.empty:
                # Partitioning theo ngày dựa trên cột updated_at
                df_new['year'] = df_new['updated_at'].dt.year
                df_new['month'] = df_new['updated_at'].dt.month
                df_new['day'] = df_new['updated_at'].dt.day
                
                # Chuẩn hóa dữ liệu: Chuyển đổi các cột object sang string và xử lý giá trị None/nan
                for col in df_new.columns:
                    if df_new[col].dtype == 'object':
                        df_new[col] = df_new[col].astype(str).replace(['None', 'nan'], [None, None])

                # Upsert vào Delta Lake
                try:
                    dt = DeltaTable(self.delta_path, storage_options=self.storage_options)
                    res = dt.merge(
                        source=df_new,
                        predicate=f"target.{self.primary_key} = source.{self.primary_key}",
                        source_alias="source", 
                        target_alias="target"
                    ).when_matched_update_all().when_not_matched_insert_all().execute()
                    
                    rows_ins = res.get("num_target_rows_inserted", 0)
                    rows_upd = res.get("num_target_rows_updated", 0)
                except:
                    # Nếu bảng Delta chưa tồn tại, tạo mới với toàn bộ dữ liệu
                    write_deltalake(self.delta_path, df_new, 
                                    mode="overwrite", 
                                    partition_by=["year", "month", "day"], 
                                    storage_options=self.storage_options,
                                    schema_mode="merge"),
                    rows_ins = len(df_new)

                final_checkpoint = df_raw['updated_at'].max().isoformat()
            else:
                # Nếu không có dữ liệu mới nào sau khi loại bỏ trùng lặp, checkpoint sẽ được đẩy lên theo window để tránh bị kẹt ở cùng một điểm
                status = "NO_ACTION_REQUIRED"
                new_cp = last_partition + timedelta(minutes=win_min - 1)
                final_checkpoint = (new_cp if new_cp < now else now - timedelta(minutes=1)).isoformat()
        else:
            # Nếu không có dữ liệu nào được trả về từ OLTP, checkpoint sẽ được đẩy lên theo window để tránh bị kẹt ở cùng một điểm
            status = "EMPTY_SOURCE"
            new_cp = last_partition + timedelta(minutes=win_min - 1)
            final_checkpoint = (new_cp if new_cp < now else now - timedelta(minutes=1)).isoformat()

        # Metadata
        duration = (datetime.now() - start_time).total_seconds()
        metrics = {
            "start_time": start_time.isoformat(),
            "duration_seconds": duration,
            "oltp_total_rows": oltp_count,
            "duplicate_rows_removed": dup_removed,
            "rows_inserted": rows_ins,
            "rows_updated": rows_upd,
            "status": status
        }
        self.save_metadata(final_checkpoint, metrics)

# --- THỰC THI CHO NHIỀU BẢNG ---
if __name__ == "__main__":
    # Liệt kê danh sách các bảng
    tables_to_sync = [
        {"name": "orders", "pk": "order_id"},
        {"name": "users", "pk": "user_id"},
        {"name": "products", "pk": "product_id"}
    ]
    
    for table in tables_to_sync:
        etl = DeltaETL(table_name=table["name"], primary_key=table["pk"])
        etl.run()