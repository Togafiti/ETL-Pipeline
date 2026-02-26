import clickhouse_connect
import logging

logging.basicConfig(level=logging.INFO)

# Cấu hình kết nối ClickHouse
# Nếu Airflow và ClickHouse cùng mạng Docker, host='clickhouse'
client = clickhouse_connect.get_client(
    host='host.docker.internal', # Thay localhost -> clickhouse
    port=8123,
    username='default',
    password='',
    database='ecommerce_db'
)

# MINIO_URL này ClickHouse dùng để kéo data, phải trỏ tới service minio
MINIO_URL = "http://minio:9000" 
BUCKET = "silver"
PREFIX = "ecommerce_db" 

TABLES = ["categories", "products", "orders", "order_items", "product_reviews"]

def sync_table(table_name):
    # Sử dụng {**} để quét đệ quy các phân vùng year/month
    s3_path = f"{MINIO_URL}/{BUCKET}/{PREFIX}/{table_name}/{{**}}/*.parquet"
    
    # Dùng TRUNCATE trước nếu bạn muốn sync toàn bộ bản mới nhất từ Silver (Overwrite)
    # Hoặc bỏ qua nếu bảng ClickHouse là ReplacingMergeTree
    client.command(f"TRUNCATE TABLE IF EXISTS silver_{table_name}")

    query = f"""
    INSERT INTO silver_{table_name}
    SELECT *
    FROM s3(
        '{s3_path}',
        'admin',
        'password',
        'Parquet'
    )
    SETTINGS input_format_null_as_default=1,
             input_format_allow_errors_num=10
    """
    try:
        client.command(query)
        logging.info(f"✅ Synced table: {table_name}")
    except Exception as e:
        logging.error(f"❌ Failed to sync {table_name}: {e}")
        raise e # Raise để Airflow task thất bại

def run_pipeline():
    for table in TABLES:
        sync_table(table)
    logging.info("🚀 All tables synced successfully!")

if __name__ == "__main__":
    run_pipeline()