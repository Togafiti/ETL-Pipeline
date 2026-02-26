import polars as pl
import s3fs
import re
import pyarrow.dataset as ds
import pyarrow as pa
from datetime import datetime, timedelta

# --- CẤU HÌNH KẾT NỐI ---
S3_OPTS = {
    "endpoint_url": "http://localhost:9000",
    "access_key_id": "admin",
    "secret_access_key": "password",
}
BUCKET_BRONZE = "bronze"
BUCKET_SILVER = "silver"

fs = s3fs.S3FileSystem(
    key=S3_OPTS["access_key_id"], 
    secret=S3_OPTS["secret_access_key"], 
    client_kwargs={'endpoint_url': S3_OPTS["endpoint_url"]}
)

def normalize_data(lf: pl.LazyFrame) -> pl.LazyFrame:
    # Xử lý String: Xóa khoảng trắng, chuyển rỗng thành Null
    return lf.with_columns([
        pl.col(pl.String).str.strip_chars().map_elements(lambda x: None if x == "" else x, return_dtype=pl.String)
    ])

def custom_cleaning(lf: pl.LazyFrame, table_name: str) -> pl.LazyFrame:
    unit = "us" 

    if "users" in table_name:
        return lf.with_columns([
            pl.from_epoch("registration_date", time_unit=unit),
            pl.col("email").str.to_lowercase(),
            pl.col("country").fill_null("Unknown")
        ])
        
    elif "orders" in table_name:
        return lf.with_columns([
            pl.from_epoch("created_at", time_unit=unit),
            pl.from_epoch("updated_at", time_unit=unit),
            pl.col("status").str.to_uppercase()
        ])
        
    elif any(t in table_name for t in ["categories", "products", "product_reviews"]):
        return lf.with_columns([
            pl.from_epoch("created_at", time_unit=unit)
        ])
        
    return lf

def run_pipeline():
    # Lấy ngày hiện tại để lọc Raw dữ liệu mới
    # 1. Lấy danh sách ngày: Hôm nay và Hôm qua
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    dates_to_process = [
        {"y": yesterday.strftime("%Y"), "m": yesterday.strftime("%m"), "d": yesterday.strftime("%d")},
        {"y": today.strftime("%Y"), "m": today.strftime("%m"), "d": today.strftime("%d")}
    ]
    cur_year, cur_month = today.strftime("%Y"), today.strftime("%m")

    base_raw_path = f"{BUCKET_BRONZE}/ecommerce"
    topic_dirs = fs.ls(base_raw_path)

    for t_dir in topic_dirs:
        match = re.search(r'cdc\.([^.]+)\.([^/]+)', t_dir)
        if not match: continue
        
        db_name, table_name = match.group(1), match.group(2)
        specific_id = f"{table_name.rstrip('s')}_id"
        
        # Cập nhật đường dẫn Silver path để kiểm tra và đọc dữ liệu cũ
        silver_base_uri = f"s3://{BUCKET_SILVER}/{db_name}/{table_name}"
        # Đường dẫn không có s3:// để dùng với fs.exists()
        silver_base_path_fs = f"{BUCKET_SILVER}/{db_name}/{table_name}"
        
        source_patterns = [
            f"s3://{t_dir}/year={dt['y']}/month={dt['m']}/day={dt['d']}/*.parquet" 
            for dt in dates_to_process
        ]
        
        try:
            all_files = []
            for pattern in source_patterns:
                all_files.extend(fs.glob(pattern))

            if not all_files:
                print(f"✨ Không có dữ liệu mới cho {table_name}.")
                continue

            all_files_s3 = [f"s3://{f}" if not f.startswith("s3://") else f for f in all_files]
            lf_new = pl.scan_parquet(all_files_s3, storage_options=S3_OPTS)
            
            if "after" in lf_new.collect_schema().names():
                lf_new = lf_new.unnest("after").drop(["before", "source", "op", "ts_ms", "transaction"], strict=False)

            # Kiểm tra dữ liệu cũ trong bucket SILVER
            if fs.exists(silver_base_path_fs):
                lf_old = pl.scan_parquet(f"{silver_base_uri}/**/*.parquet", storage_options=S3_OPTS)
                lf = pl.concat([lf_old, lf_new], how="diagonal")
            else:
                lf = lf_new

            # Xác định cột ID và Khử trùng lặp
            current_cols = lf.collect_schema().names()
            id_col = specific_id if specific_id in current_cols else ("id" if "id" in current_cols else current_cols[0])
            
            lf = lf.sort(id_col, descending=True).unique(subset=[id_col])

            # 4. Chuẩn hóa & Tạo cột Phân vùng (Year/Month)
            # Dùng cột 'created_at' hoặc 'updated_at' để phân vùng Silver
            # Nếu không có, ta dùng ngày hệ thống lúc xử lý (processing_date)
            time_col = "created_at" if "created_at" in current_cols else None
            
            if time_col:
                lf = lf.with_columns([
                    pl.col(time_col).cast(pl.Datetime).dt.year().alias("year"),
                    pl.col(time_col).cast(pl.Datetime).dt.month().alias("month")
                ])
            else:
                lf = lf.with_columns([
                    pl.lit(int(cur_year)).alias("year"),
                    pl.lit(int(cur_month)).alias("month")
                ])

            lf = normalize_data(lf)
            lf = custom_cleaning(lf, table_name)

            # GHI DỮ LIỆU THEO PHÂN VÙNG (Sử dụng PyArrow Dataset API)
            df_final = lf.collect()
            
            # Khởi tạo filesystem của PyArrow
            from pyarrow import fs as pafs
            pa_fs = pafs.S3FileSystem(
                endpoint_override=S3_OPTS["endpoint_url"].replace("http://", ""),
                access_key=S3_OPTS["access_key_id"],
                secret_key=S3_OPTS["secret_access_key"],
                scheme="http"
            )

            full_silver_output_path = f"{BUCKET_SILVER}/{db_name}/{table_name}"

            ds.write_dataset(
                df_final.to_arrow(),
                base_dir=full_silver_output_path, 
                basename_template="part-{i}.parquet",
                format="parquet",
                filesystem=pa_fs,
                partitioning=pa.dataset.partitioning(
                    pa.schema([("year", pa.int32()), ("month", pa.int32())]),
                    flavor="hive" 
                ),
                existing_data_behavior="delete_matching",
                max_partitions=1024
            )
            print(f"✅ Đã cập nhật {table_name} vào bucket {BUCKET_SILVER}")

        except Exception as e:
            print(f"⚠️ Lỗi tại bảng {table_name}: {e}")

if __name__ == "__main__":
    run_pipeline()