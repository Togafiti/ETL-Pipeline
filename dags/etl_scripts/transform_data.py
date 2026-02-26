import polars as pl
import s3fs
import re
import pyarrow.dataset as ds
import pyarrow as pa
from datetime import datetime, timedelta
import sys
import codecs
sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

# --- CONFIG ---
S3_OPTS = {
    "endpoint_url": "http://minio:9000", # Đã sửa cho Docker
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

def clean_and_cast_logic(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Xử lý triệt để lỗi Invalid UTF-8 bằng cách tránh decode trực tiếp
    """
    # Lấy danh sách cột và kiểu dữ liệu
    schema = lf.schema
    
    # 1. Xử lý các cột Binary (Thủ phạm gây lỗi UTF-8)
    binary_cols = [col for col, dtype in schema.items() if dtype == pl.Binary]
    
    if binary_cols:
        # Thay vì cast sang String (gây lỗi UTF-8), ta chuyển Binary -> Float64 trực tiếp 
        # Nếu không cast trực tiếp được, ta dùng map_batches để xử lý ở mức byte
        lf = lf.with_columns([
            pl.col(c).cast(pl.String, strict=False).cast(pl.Float64, strict=False).alias(c)
            for c in binary_cols
        ])

    # 2. Xử lý các cột String có chứa ký tự lỗi (nếu có)
    string_cols = [col for col, dtype in schema.items() if dtype == pl.String]
    if string_cols:
        lf = lf.with_columns([
            # Dùng biểu thức encode/decode để dọn dẹp các ký tự không phải UTF-8
            pl.col(c).str.strip_chars().map_elements(
                lambda x: x.encode('utf-8', 'ignore').decode('utf-8') if x else None, 
                return_dtype=pl.String
            )
            for c in string_cols
        ])
    
    return lf

def custom_cleaning(lf: pl.LazyFrame, table_name: str) -> pl.LazyFrame:
    schema = lf.schema

    def safe_date_conversion(col_name):
        # Chỉ chuyển epoch nếu cột là số (dữ liệu mới)
        if col_name in schema and schema[col_name].is_integer():
            return pl.from_epoch(col_name, time_unit="ms").dt.strftime("%Y-%m-%d %H:%M:%S")
        return pl.col(col_name)

    # Áp dụng logic chuyển đổi ngày tháng an toàn
    if "users" in table_name:
        lf = lf.with_columns([
            safe_date_conversion("registration_date"),
            pl.col("email").str.to_lowercase() if "email" in schema else pl.lit(None),
            pl.col("country").fill_null("Unknown") if "country" in schema else pl.lit("Unknown")
        ])
    elif "orders" in table_name:
        lf = lf.with_columns([
            safe_date_conversion("created_at"),
            safe_date_conversion("updated_at"),
            pl.col("status").str.to_uppercase() if "status" in schema else pl.lit(None)
        ])
    elif any(t in table_name for t in ["categories", "products", "product_reviews", "order_items"]):
        if "created_at" in schema:
            lf = lf.with_columns([safe_date_conversion("created_at")])
            
    return lf

def run_pipeline():
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    dates_to_process = [
        {"y": yesterday.strftime("%Y"), "m": yesterday.strftime("%m"), "d": yesterday.strftime("%d")},
        {"y": today.strftime("%Y"), "m": today.strftime("%m"), "d": today.strftime("%d")}
    ]

    base_raw_path = f"{BUCKET_BRONZE}/ecommerce"
    try:
        topic_dirs = fs.ls(base_raw_path)
    except: return

    for t_dir in topic_dirs:
        match = re.search(r'cdc\.([^.]+)\.([^/]+)', t_dir)
        if not match: continue
        
        db_name, table_name = match.group(1), match.group(2)
        silver_uri = f"s3://{BUCKET_SILVER}/{db_name}/{table_name}"
        silver_path_fs = f"{BUCKET_SILVER}/{db_name}/{table_name}"
        
        # Glob tìm file raw
        source_patterns = [f"s3://{t_dir}/year={dt['y']}/month={dt['m']}/day={dt['d']}/*.parquet" for dt in dates_to_process]
        all_files = []
        for p in source_patterns: all_files.extend(fs.glob(p))
        if not all_files: continue

        try:
            # --- XỬ LÝ DỮ LIỆU MỚI ---
            lf_new = pl.scan_parquet(
                [f"s3://{f}" for f in all_files], 
                storage_options=S3_OPTS,
                low_memory=True,
                cache=False
            )
            if "after" in lf_new.columns:
                lf_new = lf_new.unnest("after").drop(["before", "source", "op", "ts_ms", "transaction"], strict=False)
            
            lf_new = clean_and_cast_logic(lf_new)

            # --- MERGE VỚI SILVER ---
            if fs.exists(silver_path_fs):
                lf_old = pl.scan_parquet(f"{silver_uri}/**/*.parquet", storage_options=S3_OPTS)
                # Đảm bảo cũ và mới cùng định dạng trước khi concat
                lf_old = clean_and_cast_logic(lf_old)
                lf = pl.concat([lf_old, lf_new], how="diagonal")
            else:
                lf = lf_new

            # Khử trùng lặp
            id_col = f"{table_name.rstrip('s')}_id"
            if id_col not in lf.columns: id_col = "id" if "id" in lf.columns else lf.columns[0]
            lf = lf.sort(id_col, descending=True).unique(subset=[id_col])

            # Custom cleaning & Partition
            lf = custom_cleaning(lf, table_name)
            lf = lf.with_columns([
                pl.lit(int(today.strftime("%Y"))).alias("year"),
                pl.lit(int(today.strftime("%m"))).alias("month")
            ])

            # --- WRITE ---
            from pyarrow import fs as pafs
            pa_fs = pafs.S3FileSystem(endpoint_override=S3_OPTS["endpoint_url"].replace("http://", ""),
                                      access_key=S3_OPTS["access_key_id"], secret_key=S3_OPTS["secret_access_key"], scheme="http")

            ds.write_dataset(
                lf.collect().to_arrow(),
                base_dir=silver_path_fs,
                basename_template="part-{i}.parquet",
                format="parquet",
                filesystem=pa_fs,
                partitioning=pa.dataset.partitioning(pa.schema([("year", pa.int32()), ("month", pa.int32())]), flavor="hive"),
                existing_data_behavior="delete_matching"
            )
            print(f"✅ Finished: {table_name}")

        except Exception as e:
            print(f"❌ Error {table_name}: {e}")

if __name__ == "__main__":
    run_pipeline()