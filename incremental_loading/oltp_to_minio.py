import pandas as pd
import os
import hashlib
from datetime import datetime, timedelta, timezone
from io import BytesIO
from dataclasses import dataclass
from typing import Optional, List, Dict

from botocore.exceptions import ClientError

from etl_utils import (
    db_session,
    s3_session,
    BaseTableConfig,
    get_latest_checkpoint_bronze,
    save_checkpoint,
    add_partitions,
    get_partition_paths,
    get_latest_layer_metadata,
    diff_schema_columns,
    notify_schema_event,
)

from dotenv import load_dotenv

load_dotenv(override=True)

# --- DATACLASS CHO TABLE CONFIG (Extend BaseTableConfig) ---
@dataclass
class OLTPTableConfig(BaseTableConfig):
    """Cấu hình cho mỗi bảng trong pipeline OLTP->Bronze.
    
    Extends BaseTableConfig với các thuộc tính cơ bản:
    - table_name: Tên bảng OLTP để extract dữ liệu
    - pk_col: Tên cột primary key của bảng
    - updated_col: Tên cột timestamp để tracking thay đổi dữ liệu
    
    Dataclass này dùng để truyền cấu hình vào GenericETL một cách type-safe.
    """
    pass

class GenericETL:
    def __init__(self, config: OLTPTableConfig):
        """Khởi tạo Bronze ETL processor với cấu hình bảng.
        
        Args:
            config: OLTPTableConfig instance chứa:
                - table_name: Tên bảng OLTP nguồn
                - pk_col: Tên cột primary key (để deduplication)
                - updated_col: Tên cột timestamp (để incremental loading)
        """
        self.config = config
        self.table_name = config.table_name
        self.pk_col = config.pk_col
        self.updated_col = config.updated_col
        
        # Cấu hình kết nối
        self.bucket = os.getenv("ILOADING_BRONZE_BUCKET_NAME", "bronze-3")
        self.window_min = int(os.getenv("WINDOW_MINUTES", 30))
        self.delta_min = int(os.getenv("DELTA_MINUTES", 10))
        self.initial_start = os.getenv("INITIAL_START", "2026-03-02T00:00:00")
        self.tmp_prefix = os.getenv("BRONZE_TMP_PREFIX", "_tmp")
        self.tmp_retention_hours = int(os.getenv("TMP_RETENTION_HOURS", 24))

    def get_latest_checkpoint(self, s3):
        """Lấy checkpoint mới nhất từ S3 metadata để xác định điểm bắt đầu cho run tiếp theo.
        
        Args:
            s3: Boto3 S3 client instance
        
        Returns:
            str: ISO format timestamp của lần extract cuối cùng thành công.
                 Nếu chưa có checkpoint, trả về INITIAL_START từ env.
        
        Note:
            Checkpoint được lưu trong {table_name}/metadata/metadata_*.json
            và được sắp xếp theo LastModified để tìm file mới nhất.
        """
        checkpoint = get_latest_checkpoint_bronze(s3, self.bucket, self.table_name, self.initial_start)
        print(f"Loaded checkpoint: {checkpoint}")
        return checkpoint

    def get_fingerprints_in_range(self, s3, start_dt, end_dt):
        """Scan tất cả fingerprint (PK + UpdatedAt) trong time range để deduplication.
        
        Args:
            start_dt: Lower bound timestamp (aware datetime) của window cần scan
            end_dt: Upper bound timestamp (aware datetime) của window cần scan
        
        Returns:
            set: Set of tuples (pk_value, updated_at_timestamp) đại diện cho các record đã tồn tại.
        
        Logic:
            1. Scan tất cả partition folders trong date range
            2. Chỉ đọc file có upper_bound >= start_time (optimization)
            3. Filter record có updated_at >= start_dt
            4. Tạo set fingerprint để so sánh với batch mới
        
        Note:
            Hàm này rất quan trọng để tránh duplicate khi overlap window được sử dụng.
            Overlap window đảm bảo không miss data do late-arriving records.
        """
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
                        
                        # Lọc bớt trong DataFrame chỉ lấy những dòng thực sự nằm trong start_dt
                        df_tmp = df_tmp[df_tmp[self.updated_col] >= start_dt]
                        
                        fps = list(zip(df_tmp[self.pk_col], df_tmp[self.updated_col]))
                        existing_fps.update(fps)
                        
        return existing_fps

    def _build_stage_key(self, final_key: str, run_id: str) -> str:
        """Sinh S3 key cho staging file trong two-phase commit pattern.
        
        Args:
            final_key: S3 key cuối cùng mà file sẽ được publish (e.g., orders/year=2026/month=03/day=09/data_152030.parquet)
            run_id: Unique identifier của ETL run (format: YYYYMMDD_HHMMSS_timestamp)
        
        Returns:
            str: Staging key format: {tmp_prefix}/{table_name}/{run_id}/{partition_path}/_tmp_data_{timestamp}.parquet
        
        Purpose:
            Staging files được write riêng biệt trước, sau đó mới commit sang final key.
            Điều này đảm bảo final keys chỉ chứa file hoàn chỉnh, tránh corrupt data nếu upload bị interrupt.
        """
        file_name = final_key.split('/')[-1]
        stage_file_name = file_name.replace("data_", "_tmp_data_", 1)
        final_dir = final_key.rsplit('/', 1)[0]
        return f"{self.tmp_prefix}/{self.table_name}/{run_id}/{final_dir}/{stage_file_name}"

    @staticmethod
    def _to_parquet_payload(df: pd.DataFrame) -> bytes:
        """Serialize DataFrame thành parquet bytes để hỗ trợ content-based idempotent checking.
        
        Args:
            df: DataFrame cần serialize
        
        Returns:
            bytes: Parquet binary payload, không bao gồm index
        
        Note:
            Parquet format deterministic với cùng data + schema sẽ tạo ra cùng bytes.
            Điều này cho phép hash content để so sánh file duplicate mà không cần re-download.
        """
        buf = BytesIO()
        df.to_parquet(buf, index=False)
        return buf.getvalue()

    @staticmethod
    def _compute_payload_hash(payload: bytes) -> str:
        """Tính SHA256 hash của payload để idempotent detection.
        
        Args:
            payload: Binary content cần hash (thường là parquet bytes)
        
        Returns:
            str: Hexadecimal digest của SHA256 hash (64 characters)
        
        Use Case:
            - Retry protection: Nếu run lại với cùng data, skip upload nếu hash khớp
            - Conflict detection: Nếu final key đã tồn tại nhưng hash khác → raise error
        """
        return hashlib.sha256(payload).hexdigest()

    def _head_object_optional(self, s3, key: str) -> Optional[dict]:
        """Get S3 object metadata mà không raise exception nếu object không tồn tại.
        
        Args:
            s3: Boto3 S3 client instance
            key: S3 key cần kiểm tra
        
        Returns:
            dict: Head object response chứa ContentLength, Metadata, etc. nếu object exists
            None: Nếu object chưa tồn tại (404, NoSuchKey, NotFound)
        
        Raises:
            ClientError: Nếu lỗi khác ngoài 404 (ví dụ: permission denied)
        
        Note:
            Hàm này dùng để kiểm tra idempotent trước khi commit staging file.
        """
        try:
            return s3.head_object(Bucket=self.bucket, Key=key)
        except ClientError as exc:
            code = exc.response.get('Error', {}).get('Code', '')
            if code in {'404', 'NoSuchKey', 'NotFound'}:
                return None
            raise

    @staticmethod
    def _is_same_content(head_obj: dict, payload_hash: str, payload_size: int) -> bool:
        """So sánh content hash và size để xác định object có identical content.
        
        Args:
            head_obj: Dict response từ head_object() chứa Metadata và ContentLength
            payload_hash: SHA256 hash của payload cần so sánh
            payload_size: Size (bytes) của payload cần so sánh
        
        Returns:
            bool: True nếu object có cùng content-hash và ContentLength
        
        Note:
            So sánh cả hash VÀ size để tăng độ tin cậy (double check).
            Metadata 'content-hash' được set khi upload staged object.
        """
        metadata = head_obj.get("Metadata", {}) or {}
        existing_hash = metadata.get("content-hash")
        existing_size = int(head_obj.get("ContentLength", -1))
        return existing_hash == payload_hash and existing_size == payload_size

    def _validate_uploaded_object(self, s3, key: str) -> int:
        """Validate S3 object tồn tại và có content hợp lệ (size > 0).
        
        Args:
            s3: Boto3 S3 client instance
            key: S3 key cần validate
        
        Returns:
            int: ContentLength của object (bytes)
        
        Raises:
            ValueError: Nếu object rỗng (ContentLength <= 0)
            ClientError: Nếu object không tồn tại hoặc permission denied
        
        Note:
            Hàm này được gọi ngay sau upload để detect corrupted upload sớm.
        """
        head = s3.head_object(Bucket=self.bucket, Key=key)
        size = int(head.get("ContentLength", 0))
        if size <= 0:
            raise ValueError(f"Uploaded object is empty: {key}")
        return size

    def _upload_staged_object(self, s3, staged_key: str, payload: bytes, payload_hash: str, row_count: int):
        """Upload parquet payload lên staging area với metadata tracking.
        
        Args:
            s3: Boto3 S3 client instance
            staged_key: S3 key trong tmp prefix để upload
            payload: Parquet binary content
            payload_hash: SHA256 hash của payload (để idempotent check)
            row_count: Số dòng data trong payload (để metadata tracking)
        
        Raises:
            ValueError: Nếu uploaded size không khớp với payload size (corrupted upload)
        
        Metadata được set:
            - content-hash: SHA256 hash để idempotent validation
            - row-count: Số dòng data để monitoring
        
        Note:
            Staging upload luôn được validate ngay để detect error sớm trước khi commit.
        """
        s3.put_object(
            Bucket=self.bucket,
            Key=staged_key,
            Body=payload,
            Metadata={
                "content-hash": payload_hash,
                "row-count": str(row_count),
            },
        )

        staged_size = self._validate_uploaded_object(s3, staged_key)
        if staged_size != len(payload):
            raise ValueError(
                f"Uploaded staged object size mismatch for {staged_key}: {staged_size} != {len(payload)}"
            )

    def _commit_staged_object(self, s3, staged_key: str, final_key: str):
        """Atomically promote staged file thành final published file.
        
        Args:
            s3: Boto3 S3 client instance
            staged_key: S3 key của file đang ở staging area
            final_key: S3 key đích để publish file
        
        Process:
            1. Copy staged object → final key (với MetadataDirective='COPY')
            2. Validate final object tồn tại và có content hợp lệ
        
        Note:
            - S3 không có rename operation, phải dùng copy
            - Staged key sẽ được cleanup sau khi transaction hoàn tất (trong finally block)
            - Nếu validate fail, exception sẽ trigger rollback toàn bộ transaction
        """
        s3.copy_object(
            Bucket=self.bucket,
            CopySource={"Bucket": self.bucket, "Key": staged_key},
            Key=final_key,
            MetadataDirective='COPY',
        )
        self._validate_uploaded_object(s3, final_key)

    def _delete_objects_safe(self, s3, keys: List[str]):
        """Batch delete S3 objects với automatic chunking để tránh API limit.
        
        Args:
            s3: Boto3 S3 client instance
            keys: List các S3 keys cần xóa (filter None/empty trước khi xóa)
        
        Note:
            - S3 delete_objects API giới hạn 1000 objects/request
            - Hàm này tự động chia thành chunks 1000 để handle large batch
            - Dùng cho cleanup staging files hoặc rollback failed transactions
        """
        keys = [k for k in keys if k]
        if not keys:
            return

        for i in range(0, len(keys), 1000):
            chunk = keys[i:i + 1000]
            s3.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": [{"Key": k} for k in chunk]},
            )

    def _build_transaction_items(
        self,
        df_diff: pd.DataFrame,
        upper_bound: datetime,
        run_id: str,
    ) -> List[Dict[str, object]]:
        """Chuẩn bị transaction plan chứa tất cả partitions cần write trong time window.
        
        Args:
            df_diff: DataFrame chứa new/changed records sau deduplication
            upper_bound: Upper bound timestamp của window (dùng làm file suffix)
            run_id: Unique identifier của run (dùng để tạo staging paths)
        
        Returns:
            List[Dict]: Mỗi item chứa:
                - final_key: S3 key cuối cùng để publish
                - staged_key: S3 key tạm trong staging area
                - payload: Parquet bytes của partition data
                - payload_hash: SHA256 hash của payload
                - payload_size: Size (bytes) của payload
                - row_count: Số dòng data trong partition
        
        Note:
            Transaction plan này cho phép validate toàn bộ window trước khi commit.
            All-or-nothing commit pattern đảm bảo không có partial window được publish.
        """
        tx_items = []
        suffix = f"/data_{upper_bound.strftime('%H%M%S')}.parquet"
        for final_key, group in get_partition_paths(df_diff, self.table_name, suffix=suffix):
            payload = self._to_parquet_payload(group)
            payload_hash = self._compute_payload_hash(payload)
            staged_key = self._build_stage_key(final_key, run_id)
            tx_items.append(
                {
                    "final_key": final_key,
                    "staged_key": staged_key,
                    "payload": payload,
                    "payload_hash": payload_hash,
                    "payload_size": len(payload),
                    "row_count": len(group),
                }
            )
        return tx_items

    def _write_partitions_transactional(self, s3, tx_items: List[Dict[str, object]]):
        """Execute two-phase commit với all-or-nothing guarantee cho toàn bộ window.
        
        Args:
            s3: Boto3 S3 client instance
            tx_items: List transaction items từ _build_transaction_items()
        
        Transaction Phases:
            Phase 1 - STAGE ALL:
                - Upload tất cả partitions lên staging area
                - Validate từng staged file ngay sau upload
            
            Phase 2 - VALIDATE:
                - Kiểm tra idempotent: nếu final key đã tồn tại với cùng hash → skip
                - Conflict detection: nếu final key tồn tại với hash khác → FAIL ENTIRE TRANSACTION
            
            Phase 3 - COMMIT ALL:
                - Copy tất cả staged files → final keys
                - Track committed keys để rollback nếu lỗi giữa chừng
            
            Phase 4 - CLEANUP (finally):
                - Xóa tất cả staged files (thành công hay thất bại)
        
        Rollback Logic:
            - Nếu bất kỳ step nào fail sau khi bắt đầu commit:
                → Xóa tất cả final keys đã commit trong transaction này
                → Re-raise exception để caller biết transaction failed
        
        Note:
            Đây là implementation của partial failures pattern cho Bronze layer.
            Window phải được commit hoàn toàn hoặc rollback hoàn toàn, không có partial state.
        """
        if not tx_items:
            return

        staged_keys = [item["staged_key"] for item in tx_items]
        committed_new_keys = []

        try:
            # Phase 1: stage toàn bộ object trước.
            for item in tx_items:
                self._upload_staged_object(
                    s3,
                    staged_key=item["staged_key"],
                    payload=item["payload"],
                    payload_hash=item["payload_hash"],
                    row_count=item["row_count"],
                )
                print(f"Staged file: {item['staged_key']} ({item['payload_size']} bytes)")

            # Validation trước commit: nếu final key đã tồn tại và khác content thì fail transaction.
            for item in tx_items:
                head = self._head_object_optional(s3, item["final_key"])
                if head is None:
                    item["skip_commit"] = False
                    continue

                if self._is_same_content(head, item["payload_hash"], item["payload_size"]):
                    item["skip_commit"] = True
                    print(f"Idempotent skip: {item['final_key']}")
                    continue

                raise ValueError(
                    f"Transactional conflict: final key already exists with different content: {item['final_key']}"
                )

            # Phase 2: commit. Nếu lỗi giữa chừng thì rollback các final key mới commit.
            for item in tx_items:
                if item.get("skip_commit"):
                    continue

                self._commit_staged_object(s3, item["staged_key"], item["final_key"])
                committed_new_keys.append(item["final_key"])
                print(f"Committed file: {item['final_key']}")

        except Exception:
            if committed_new_keys:
                print(f"⚠️ Rolling back {len(committed_new_keys)} committed file(s) due to transaction failure")
                self._delete_objects_safe(s3, committed_new_keys)
            raise
        finally:
            # Cleanup toàn bộ staged keys để tránh rác nếu run bị lỗi.
            self._delete_objects_safe(s3, staged_keys)

    def _cleanup_stale_tmp_files(self, s3):
        """Periodic cleanup cho staging files cũ quá TMP_RETENTION_HOURS.
        
        Args:
            s3: Boto3 S3 client instance
        
        Logic:
            1. Scan tất cả objects trong {tmp_prefix}/{table_name}/
            2. Xóa objects có LastModified < (now - TMP_RETENTION_HOURS)
            3. Batch delete 1000 objects/lần để tuân thủ S3 API limit
        
        Use Case:
            - Nếu ETL run bị crash giữa chừng, staged files sẽ bị bỏ lại
            - Cleanup định kỳ ở đầu mỗi run để tránh tích lũy storage cost
            - Retention window cho phép debug recent failures
        
        Note:
            Cleanup failure không làm fail ETL run, chỉ log warning.
        """
        tmp_root = f"{self.tmp_prefix}/{self.table_name}/"
        cutoff = datetime.now(timezone.utc) - timedelta(hours=self.tmp_retention_hours)
        paginator = s3.get_paginator('list_objects_v2')

        delete_batch = []
        deleted_count = 0

        for page in paginator.paginate(Bucket=self.bucket, Prefix=tmp_root):
            for obj in page.get('Contents', []):
                if obj['LastModified'] < cutoff:
                    delete_batch.append({"Key": obj['Key']})

                    if len(delete_batch) == 1000:
                        s3.delete_objects(Bucket=self.bucket, Delete={"Objects": delete_batch})
                        deleted_count += len(delete_batch)
                        delete_batch = []

        if delete_batch:
            s3.delete_objects(Bucket=self.bucket, Delete={"Objects": delete_batch})
            deleted_count += len(delete_batch)

        if deleted_count > 0:
            print(f"Cleaned {deleted_count} stale tmp file(s) under {tmp_root}")

    def run(self):
        """Execute Bronze ETL trong standalone mode với connections tự quản lý.
        
        Mode:
            Standalone - tạo DB và S3 connections riêng cho ETL run này.
            Dùng khi chạy single table hoặc testing.
        
        See Also:
            run_etl_with_connections() - Shared connection mode cho multi-table pipeline
        """
        with db_session() as engine, s3_session() as s3:
            self._execute_etl(engine, s3)

    def run_etl_with_connections(self, engine, s3):
        """Execute Bronze ETL với connections được share từ caller.
        
        Args:
            engine: SQLAlchemy engine đã được tạo sẵn
            s3: Boto3 S3 client đã được tạo sẵn
        
        Mode:
            Shared - dùng connections được truyền vào từ multi-table pipeline.
            Hiệu quả hơn khi chạy nhiều tables vì tái sử dụng connection pool.
        
        See Also:
            run() - Standalone mode với connections riêng
        """
        self._execute_etl(engine, s3)

    def _execute_etl(self, engine, s3):
        """Core Bronze ETL logic: Extract từ OLTP → Deduplicate → Transactional write → S3.
        
        Args:
            engine: SQLAlchemy engine để query OLTP database
            s3: Boto3 S3 client để read/write Bronze layer
        
        Process Flow:
            1. Cleanup stale tmp files từ run trước (nếu có)
            2. Load previous schema từ metadata để detect schema drift
            3. Load checkpoint và tính time window:
                - lower_bound = checkpoint - DELTA_MINUTES (overlap để catch late-arriving)
                - upper_bound = lower_bound + WINDOW_MINUTES
            4. Extract data từ OLTP WHERE updated_col IN [lower_bound, upper_bound]
            5. Detect schema evolution (added/removed columns)
            6. Deduplicate dựa trên fingerprints (PK + updated_at) từ existing S3 data
            7. Partition theo year/month/day từ updated_col
            8. Transactional write:
                - Stage all partitions
                - Validate idempotent
                - Commit all or rollback
            9. Update checkpoint:
                - Nếu có new data: checkpoint = max(updated_at) của batch
                - Nếu empty: checkpoint = old_checkpoint + WINDOW_MINUTES - 1p (để tiến lên)
            10. Save metadata với schema columns, metrics, status
        
        Status Values:
            - SUCCESS: Có new data được written
            - NO_CHANGES: Extract có data nhưng tất cả duplicate
            - EMPTY_SOURCE: Query OLTP không trả về data
        
        Schema Evolution:
            - Compare current columns vs previous metadata
            - Notify terminal nếu có added/removed columns
            - Update metadata với schema mới
        
        Checkpoint Strategy:
            - Window-based progressive checkpoint để tránh stuck
            - Overlap window (DELTA) để catch late-arriving updates
            - Checkpoint luôn được update (không stuck) ngay cả khi no data
        """
        start_time = datetime.now()

        # Dọn các file tạm cũ ở đầu mỗi run (periodic cleanup)
        try:
            self._cleanup_stale_tmp_files(s3)
        except Exception as exc:
            print(f"⚠️  TMP cleanup skipped for {self.table_name}: {exc}")

        # Schema detection baseline từ metadata run trước
        previous_meta = get_latest_layer_metadata(s3, self.bucket, self.table_name, layer='bronze')
        previous_columns = previous_meta.get('schema_columns', [])

        last_checkpoint = datetime.fromisoformat(self.get_latest_checkpoint(s3))
        
        # Tính toán Window
        lower_bound = last_checkpoint - timedelta(minutes=self.delta_min)
        upper_bound = lower_bound + timedelta(minutes=self.window_min)
        if upper_bound > start_time: upper_bound = start_time - timedelta(seconds=60)
        
        print(f"Processing [{self.table_name}]: {lower_bound} -> {upper_bound}")

        # Extract dữ liệu mới từ OLTP dựa trên cột updated_at
        query = f"SELECT * FROM {self.table_name} WHERE {self.updated_col} > '{lower_bound}' AND {self.updated_col} <= '{upper_bound}'"
        df_raw = pd.read_sql(query, engine)

        current_columns = sorted([str(col) for col in df_raw.columns.tolist()])
        schema_diff = diff_schema_columns(previous_columns, current_columns)
        if previous_columns and schema_diff['changed']:
            msg = (
                f"Table: {self.table_name}\n"
                f"Added columns: {schema_diff['added_columns'] or '[]'}\n"
                f"Removed columns: {schema_diff['removed_columns'] or '[]'}\n"
                f"Layer: Bronze"
            )
            notify_schema_event(title="Schema Evolution Alert - Bronze", message=msg, level="WARN")
            print(f"⚠️ Schema changed for {self.table_name}: +{schema_diff['added_columns']} -{schema_diff['removed_columns']}")
        
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
                run_id = f"{start_time.strftime('%Y%m%d_%H%M%S')}_{int(start_time.timestamp())}"

                tx_items = self._build_transaction_items(df_diff, upper_bound, run_id)
                self._write_partitions_transactional(s3, tx_items)
                
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
            "status": status,
            "schema_columns": schema_diff["current_columns"],
            "schema_evolution": {
                "changed": schema_diff["changed"],
                "added_columns": schema_diff["added_columns"],
                "removed_columns": schema_diff["removed_columns"],
            },
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
            "schema_columns": integrity_metrics.get("schema_columns", []),
            "schema_evolution": integrity_metrics.get("schema_evolution", {}),
            "status": integrity_metrics.get("status", "SUCCESS"),
        }
        
        save_checkpoint(s3, self.bucket, self.table_name, metadata_data, layer='bronze')
        print(f"🏁 {integrity_metrics.get('status')} | Written: {integrity_metrics.get('delta_upserted')} | CP: {checkpoint}")

# --- CẤU HÌNH DANH SÁCH BẢNG (Sử dụng Dataclass) ---
TABLES_TO_SYNC = [
    OLTPTableConfig(
        table_name="orders", 
        pk_col="order_id", 
        updated_col="updated_at"
    ),
    OLTPTableConfig(
        table_name="users", 
        pk_col="user_id", 
        updated_col="updated_at"
    ),
    OLTPTableConfig(
        table_name="products", 
        pk_col="product_id", 
        updated_col="updated_at"
    ),
    OLTPTableConfig(
        table_name="categories", 
        pk_col="category_id", 
        updated_col="updated_at"
    ),
    OLTPTableConfig(
        table_name="order_items", 
        pk_col="item_id", 
        updated_col="updated_at"
    ),
    OLTPTableConfig(
        table_name="product_reviews", 
        pk_col="review_id", 
        updated_col="updated_at"
    )
]

def run_pipeline(configs: list):
    """Chạy ETL cho nhiều bảng, REUSE connections để tối ưu hiệu suất"""
    with db_session() as engine, s3_session() as s3:
        for config in configs:
            try:
                etl = GenericETL(config)
                print(f"Processing: {config.table_name}")
                
                # Gọi run_etl() thay vì run() để pass connections
                etl.run_etl_with_connections(engine, s3)
                
                print(f"Completed: {config.table_name}\n")
            except Exception as e:
                print(f"Failed: {config.table_name} - {e}\n")
                continue


if __name__ == "__main__":
    """Chạy ETL cho tất cả bảng với shared connections"""
    run_pipeline(TABLES_TO_SYNC)