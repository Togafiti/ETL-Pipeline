import clickhouse_connect
import logging

logging.basicConfig(level=logging.INFO)

client = clickhouse_connect.get_client(
    host='localhost',
    port=8123,
    username='default',
    password='',
    database='ecommerce_db'
)

# mapping table ClickHouse ↔ folder MinIO
TABLES = [
    "categories", 
    "products",
    "orders",
    "order_items",
    "product_reviews",
]

MINIO_URL = "http://host.docker.internal:9000"
BUCKET = "silver"
PREFIX = "ecommerce_db" 


def sync_table(table_name):
    query = f"""
    INSERT INTO silver_{table_name}
    SELECT *
    FROM s3(
        '{MINIO_URL}/{BUCKET}/{PREFIX}/{table_name}/**/*.parquet', -- Sử dụng ** để tìm đệ quy
        'admin',
        'password',
        'Parquet'
    )
    """
    client.command(query)
    logging.info(f"✅ Synced table: {table_name}")

def run_pipeline():

    for table in TABLES:
        sync_table(table)

    logging.info("🚀 All tables synced successfully!")

if __name__ == "__main__":
    run_pipeline()
