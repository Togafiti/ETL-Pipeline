import pandas as pd
from datetime import datetime
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import json
import os
import pyarrow as pa

# Cấu hình đường dẫn
BRONZE_PATH = "s3://bronze-2/orders_delta"
SILVER_PATH = "s3://silver-2/orders_cleaned_delta"
CHECKPOINT_FILE = "silver_checkpoint.json"

# STORAGE_OPTIONS (Dùng chung cấu hình MinIO như cũ)
STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY_ID": "admin",
    "AWS_SECRET_ACCESS_KEY": "password",
    "AWS_ENDPOINT_URL": "http://localhost:9000",
    "AWS_REGION": "us-east-1",
    "AWS_ALLOW_HTTP": "true",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
}

def get_last_processed_version():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return json.load(f).get("last_version", -1)
    return -1

def save_silver_checkpoint(version, metrics=None):
    """
    version: Số version của Bronze đã xử lý xong (int)
    metrics: Dictionary chứa các thông số audit (tùy chọn)
    """
    checkpoint_data = {
        "last_version": version,
        "last_run_at": datetime.now().isoformat(),
        "audit": metrics or {}  # Lưu oltp_count, strategy, v.v.
    }
    
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            json.dump(checkpoint_data, f, indent=4)
        print(f"✅ Checkpoint Silver saved at Version {version}")
    except Exception as e:
        print(f"❌ Error saving checkpoint: {e}")

def load_hybrid_to_silver():
    # 1. Khởi tạo kết nối
    dt_bronze = DeltaTable(BRONZE_PATH, storage_options=STORAGE_OPTIONS)
    current_v = dt_bronze.version()
    last_v = get_last_processed_version() # Hàm này đọc từ file JSON checkpoint của bạn

    if current_v <= last_v:
        print("Silver đã đồng bộ. Không có dữ liệu mới.")
        return

    try:
        # 2. Quy định chiến lược nạp dữ liệu
        if last_v == -1:
            # --- CHIẾN LƯỢC FULL LOAD (Từ Partitions) ---
            print(f"Bắt đầu Full Load từ Bronze Version {current_v}...")
            # to_pandas() sẽ quét các folder partition year/month
            df_to_process = dt_bronze.to_pandas()
            mode = "initial"
        else:
            # --- CHIẾN LƯỢC INCREMENTAL (Từ _change_data) ---
            print(f"Bắt đầu Incremental Load từ Version {last_v + 1} đến {current_v}...")
            changes_iter = dt_bronze.load_cdf(starting_version=last_v + 1)
            table = pa.table(changes_iter)
            df_changes = table.to_pandas()
            
            # Lọc lấy dòng mới và dòng sau khi update
            if not df_changes.empty:
                df_to_process = df_changes[
                    df_changes['_change_type'].isin(['insert', 'update_postimage'])
                ].copy()
            else:
                df_to_process = pd.DataFrame()
            mode = "incremental"

        if df_to_process.empty:
            print("Không có dòng nào cần biến đổi.")
            return

        # 3. TRANSFORM (Ví dụ logic chung)
        print(f"Đang Transform {len(df_to_process)} dòng...")
        df_to_process['status'] = df_to_process['status'].str.upper()
        df_to_process['etl_silver_at'] = pd.Timestamp.now()

        # 4. GHI VÀO SILVER
        if mode == "initial":
            # Khởi tạo bảng Silver lần đầu
            write_deltalake(
                SILVER_PATH, 
                df_to_process, 
                mode="overwrite", 
                partition_by=["year", "month"],
                storage_options=STORAGE_OPTIONS
            )
        else:
            # Loại bỏ các cột metadata của CDF (như _change_type, _commit_version...)
            cols_to_keep = [c for c in df_to_process.columns if not c.startswith('_')]
            df_to_merge = df_to_process[cols_to_keep].copy()
            # Merge phần bù vào bảng Silver đã có
            dt_silver = DeltaTable(SILVER_PATH, storage_options=STORAGE_OPTIONS)
            (
                dt_silver.merge(
                    source=df_to_merge,
                    # Sử dụng Alias 'target' và 'source' rõ ràng
                    predicate="target.order_id = source.order_id",
                    source_alias="source",
                    target_alias="target"
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )

        # 5. Cập nhật checkpoint (Lưu cả version và audit metrics)
        metrics = {
            "processed_rows": len(df_to_process),
            "strategy": mode
        }
        save_silver_checkpoint(current_v, metrics) 
        print(f"Hoàn thành cập nhật Silver lên Version {current_v}")

    except Exception as e:
        print(f"Lỗi trong quá trình Hybrid Transform: {e}")

if __name__ == "__main__":
    load_hybrid_to_silver()