import pandas as pd
import boto3
import json
import os
from datetime import datetime, timedelta
from io import BytesIO
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv(override=True)

class GenericETL:
    def __init__(self, table_name, pk_col='id', updated_col='updated_at'):
        self.table_name = table_name
        self.pk_col = pk_col
        self.updated_col = updated_col
        
        # Cấu hình kết nối (Dùng chung)
        self.bucket = os.getenv("ILOADING_BUCKET_NAME")
        self.window_min = int(os.getenv("WINDOW_MINUTES"))
        self.delta_min = int(os.getenv("DELTA_MINUTES"))
        self.initial_start = os.getenv("INITIAL_START")
        
        # Kết nối DB và S3 (Dùng chung)
        self.engine = create_engine(
            f"mysql+mysqlconnector://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
            f"{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_DATABASE')}"
        )
        self.s3 = boto3.client("s3", 
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.getenv("AWS_ENDPOINT_URL")
        )

    # Lấy checkpoint mới nhất từ S3, nếu không có thì dùng giá trị mặc định
    def get_latest_checkpoint(self):
        prefix = f"{self.table_name}/metadata/metadata_"
        try:
            # Lấy file metadata mới nhất để xác định checkpoint
            response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            # Nếu có file metadata, lấy giá trị last_updated_at từ file mới nhất
            if 'Contents' in response:
                # Sắp xếp file theo LastModified để lấy file mới nhất
                latest_file = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)[0]['Key']
                obj = self.s3.get_object(Bucket=self.bucket, Key=latest_file)
                return json.loads(obj['Body'].read().decode('utf-8'))['last_updated_at']
        except Exception as e:
            print(f"Checkpoint not found for {self.table_name}, using default.")
        return self.initial_start

    # Lấy tất cả fingerprint (PK + UpdatedAt) trong vùng overlap để loại bỏ những bản ghi trùng lặp
    def get_fingerprints_in_range(self, start_dt, end_dt):
        existing_fps = set()
        date_range = pd.date_range(start=start_dt.date(), end=end_dt.date())
        
        # Chuyển start_dt về dạng số để so sánh (ví dụ: 024151)
        start_time_int = int(start_dt.strftime('%H%M%S'))
        
        for dt in date_range:
            # Lấy tất cả file trong ngày đó dựa trên partitioning path
            prefix = f"{self.table_name}/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            paginator = self.s3.get_paginator('list_objects_v2')
            
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
                        resp = self.s3.get_object(Bucket=self.bucket, Key=file_key)
                        df_tmp = pd.read_parquet(BytesIO(resp['Body'].read()), columns=[self.pk_col, self.updated_col])
                        
                        # Chuyển cột updated_at sang datetime để so khớp chính xác
                        df_tmp[self.updated_col] = pd.to_datetime(df_tmp[self.updated_col])
                        
                        # (Tùy chọn) Lọc bớt trong DataFrame chỉ lấy những dòng thực sự nằm trong start_dt
                        df_tmp = df_tmp[df_tmp[self.updated_col] >= start_dt]
                        
                        fps = list(zip(df_tmp[self.pk_col], df_tmp[self.updated_col]))
                        existing_fps.update(fps)
                        
        return existing_fps

    # Main ETL Logic
    def run(self):
        start_time = datetime.now()
        last_checkpoint = datetime.fromisoformat(self.get_latest_checkpoint())
        
        # Tính toán Window
        lower_bound = last_checkpoint - timedelta(minutes=self.delta_min)
        upper_bound = lower_bound + timedelta(minutes=self.window_min)
        if upper_bound > start_time: upper_bound = start_time - timedelta(seconds=60)
        
        print(f"Processing [{self.table_name}]: {lower_bound} -> {upper_bound}")

        # Extract dữ liệu mới từ OLTP dựa trên cột updated_at
        query = f"SELECT * FROM {self.table_name} WHERE {self.updated_col} > '{lower_bound}' AND {self.updated_col} <= '{upper_bound}'"
        df_raw = pd.read_sql(query, self.engine)
        
        oltp_count = len(df_raw)
        dup_count = 0
        status = "SUCCESS"

        # Nếu có dữ liệu mới, tiến hành loại bỏ trùng lặp và load lên S3
        if not df_raw.empty:
            df_raw[self.updated_col] = pd.to_datetime(df_raw[self.updated_col])
            
            # Lấy tất cả fingerprint (PK + UpdatedAt) trong vùng overlap để loại bỏ những bản ghi trùng lặp
            existing_fps = self.get_fingerprints_in_range(lower_bound, upper_bound)
            df_raw['fp'] = list(zip(df_raw[self.pk_col], df_raw[self.updated_col]))
            df_diff = df_raw[~df_raw['fp'].isin(existing_fps)].copy()
            df_diff.drop(columns=['fp'], inplace=True)
            
            dup_count = oltp_count - len(df_diff)

            # Nếu có dữ liệu mới sau khi loại bỏ trùng lặp, tiến hành partitioning và load lên S3
            if not df_diff.empty:
                # Partitioning theo ngày dựa trên cột updated_at
                df_diff['y'] = df_diff[self.updated_col].dt.year
                df_diff['m'] = df_diff[self.updated_col].dt.month
                df_diff['d'] = df_diff[self.updated_col].dt.day
                
                for (y, m, d), group in df_diff.groupby(['y', 'm', 'd']):
                    path = f"{self.table_name}/year={y}/month={m:02d}/day={d:02d}/data_{upper_bound.strftime('%H%M%S')}.parquet"
                    buf = BytesIO()
                    group.drop(columns=['y', 'm', 'd']).to_parquet(buf, index=False, engine='pyarrow')
                    self.s3.put_object(Bucket=self.bucket, Key=path, Body=buf.getvalue())
                
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
        self.save_audit(final_checkpoint, integrity_metrics)

    # Lưu metadata audit lên S3 để theo dõi và làm checkpoint cho lần chạy tiếp theo
    def save_audit(self, checkpoint, integrity_metrics):
        audit_path = f"{self.table_name}/metadata/metadata_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        metadata_data = {
            "last_updated_at": checkpoint,  # Giá trị cột updated_at lớn nhất từ DB
            "etl_performance": {
                "start_time": integrity_metrics.get("start_time"), # Thời điểm bắt đầu của window
                "end_time": integrity_metrics.get("end_time"), # Thời điểm kết thúc của window
            },
            "integrity_check": {
                "oltp_total_rows": integrity_metrics.get("oltp_total_rows", 0),  # Tổng dòng lấy từ DB
                "duplicate_rows_removed": integrity_metrics.get("duplicate_rows_removed", 0), # Số dòng trùng bị loại
                "delta_upserted_total": integrity_metrics.get("delta_upserted", 0), # Tổng dòng thực tế đưa vào Delta
            },
            "status": integrity_metrics.get("status", "SUCCESS")
        }

        self.s3.put_object(Bucket=self.bucket, Key=audit_path, Body=json.dumps(metadata_data, indent=4))
        print(f"🏁 {integrity_metrics.get('status')} | Written: {integrity_metrics.get('delta_upserted')} | CP: {checkpoint}")

# Chạy ETL cho từng bảng được cấu hình trong danh sách
if __name__ == "__main__":
    # Danh sách các bảng cần chạy
    tables_to_sync = [
        {"name": "orders", "pk": "order_id", "updated": "updated_at"},
        {"name": "users", "pk": "user_id", "updated": "updated_at"},
        {"name": "products", "pk": "sku", "updated": "updated_at"}
    ]
    
    for config in tables_to_sync:
        etl = GenericETL(config['name'], config['pk'], config['updated'])
        etl.run()