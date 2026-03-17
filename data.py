import random
import os
import mysql.connector
from faker import Faker
from datetime import datetime, timedelta, date
import uuid

fake = Faker()
FIXED_CREATED_AT = datetime(2026, 1, 1, 0, 0, 0)

# ---------------------------------------------------------------------------
# 1. Connection
# ---------------------------------------------------------------------------
conn = mysql.connector.connect(
    host="localhost",
    port=3307,
    user="root",
    password="123456",
    database="station_traffic",
    autocommit=False,
)
cur = conn.cursor(dictionary=True)

# ---------------------------------------------------------------------------
# 2. Global state caches
# ---------------------------------------------------------------------------
existing_operator_ids: list = []
existing_stations: list = []   # list of dicts: {id, station_code, operator_id}


def _read_ratio(env_name, default_value):
    try:
        value = float(os.getenv(env_name, str(default_value)))
    except ValueError:
        value = default_value
    return max(0.0, min(1.0, value))


# ---------------------------------------------------------------------------
# 3. Dirty-data controls (set ratio to 0 to disable per table)
# ---------------------------------------------------------------------------
ENABLE_BAD_DATA    = os.getenv("ENABLE_BAD_DATA", "1") == "1"
BAD_STATION_RATIO  = _read_ratio("BAD_STATION_RATIO",  0.12)
BAD_TRAFFIC_RATIO  = _read_ratio("BAD_TRAFFIC_RATIO",  0.15)
ENABLE_RANDOM_MUTATIONS = os.getenv("ENABLE_RANDOM_MUTATIONS", "1") == "1"
MAX_MUTATION_ROWS_PER_TABLE = max(1, int(os.getenv("MAX_MUTATION_ROWS_PER_TABLE", "5")))
MAX_ROW_ERROR_LOGS = int(os.getenv("MAX_ROW_ERROR_LOGS", "20"))

dirty_stats = {
    "stations":    0,
    "traffic":     0,
    "failed_rows": 0,
}
mutation_stats = {
    "operators_updated": 0,
    "operators_deleted": 0,
    "stations_updated": 0,
    "stations_deleted": 0,
    "traffic_updated": 0,
    "traffic_deleted": 0,
}
row_error_log_count = 0


def current_timestamp() -> datetime:
    return datetime.now()

# ---------------------------------------------------------------------------
# 4. Date helpers
# ---------------------------------------------------------------------------
def random_event_time() -> datetime:
    """Random datetime within 2026 up to now."""
    start = datetime(2026, 1, 1)
    end   = datetime.now()
    delta_secs = max(1, int((end - start).total_seconds()))
    return start + timedelta(seconds=random.randrange(delta_secs))


def random_future_date(max_days: int = 120) -> datetime:
    return datetime.now() + timedelta(
        days=random.randint(1, max_days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


def random_old_date(start_year: int = 2004, end_year: int = 2014) -> datetime:
    start = datetime(start_year, 1, 1)
    end   = datetime(end_year, 12, 31, 23, 59, 59)
    secs  = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randrange(secs))


def random_future_date_only(max_days: int = 730) -> date:
    return (datetime.now() + timedelta(days=random.randint(1, max_days))).date()


# ---------------------------------------------------------------------------
# 5. Savepoint / logging helpers (unchanged pattern)
# ---------------------------------------------------------------------------
def choose_dirty_case(ratio: float, cases: list):
    if not ENABLE_BAD_DATA or random.random() >= ratio:
        return None
    return random.choice(cases)


def new_savepoint(prefix):
    return f"{prefix}_{uuid.uuid4().hex[:10]}"


def safe_release_savepoint(savepoint_name):
    try:
        cur.execute(f"RELEASE SAVEPOINT {savepoint_name}")
    except Exception:
        pass


def log_row_error(scope, err):
    global row_error_log_count
    if row_error_log_count < MAX_ROW_ERROR_LOGS:
        print(f"[WARN] Skip row in {scope}: {err}")
        row_error_log_count += 1
    elif row_error_log_count == MAX_ROW_ERROR_LOGS:
        print("[WARN] Too many row-level errors. Further row errors are suppressed.")
        row_error_log_count += 1


# ---------------------------------------------------------------------------
# 6. Per-table corruption functions
# ---------------------------------------------------------------------------
def maybe_corrupt_station_payload(
    province: str,
    district: str,
    lat: float,
    lon: float,
    install_date: date,
):
    dirty_case = choose_dirty_case(
        BAD_STATION_RATIO,
        [
            "out_of_range_lat",
            "out_of_range_lon",
            "negative_coordinates",
            "future_install_date",
            "blank_province",
            "blank_district",
        ],
    )

    if dirty_case == "out_of_range_lat":
        lat = random.uniform(91.0, 180.0)
    elif dirty_case == "out_of_range_lon":
        lon = random.uniform(181.0, 360.0)
    elif dirty_case == "negative_coordinates":
        lat = -abs(lat)
        lon = -abs(lon)
    elif dirty_case == "future_install_date":
        install_date = random_future_date_only(730)
    elif dirty_case == "blank_province":
        province = ""
    elif dirty_case == "blank_district":
        district = ""

    return province, district, lat, lon, install_date, dirty_case


def maybe_corrupt_traffic_payload(
    technology: str,
    traffic_gb: float,
    user_count: int,
    event_time: datetime,
):
    dirty_case = choose_dirty_case(
        BAD_TRAFFIC_RATIO,
        [
            "negative_traffic_gb",
            "extreme_traffic_gb",
            "zero_user_count_high_traffic",
            "invalid_technology",
            "future_event_time",
            "very_old_event_time",
        ],
    )

    if dirty_case == "negative_traffic_gb":
        traffic_gb = -abs(traffic_gb)
    elif dirty_case == "extreme_traffic_gb":
        traffic_gb = round(traffic_gb * random.uniform(50.0, 200.0), 4)
    elif dirty_case == "zero_user_count_high_traffic":
        user_count = 0
        traffic_gb = round(random.uniform(100.0, 500.0), 4)
    elif dirty_case == "invalid_technology":
        technology = random.choice(["6G", "7G", "WIFI6", "??", ""])
    elif dirty_case == "future_event_time":
        event_time = random_future_date(60)
    elif dirty_case == "very_old_event_time":
        event_time = random_old_date()

    return technology, traffic_gb, user_count, event_time, dirty_case


def print_dirty_summary() -> None:
    print("\n=== Insert Summary ===")
    print(f"bad_data_enabled: {ENABLE_BAD_DATA}")
    for key in ["stations", "traffic"]:
        print(f"dirty_{key}: {dirty_stats[key]}")
    print(f"failed_rows_skipped: {dirty_stats['failed_rows']}")


def print_mutation_summary() -> None:
    print("\n=== Mutation Summary ===")
    print(f"random_mutations_enabled: {ENABLE_RANDOM_MUTATIONS}")
    print(f"operators_updated: {mutation_stats['operators_updated']}")
    print(f"operators_soft_deleted: {mutation_stats['operators_deleted']}")
    print(f"stations_updated: {mutation_stats['stations_updated']}")
    print(f"stations_soft_deleted: {mutation_stats['stations_deleted']}")
    print(f"traffic_updated: {mutation_stats['traffic_updated']}")
    print(f"traffic_soft_deleted: {mutation_stats['traffic_deleted']}")


# ---------------------------------------------------------------------------
# 7. Reference data
# ---------------------------------------------------------------------------
# Vietnamese provinces and districts for geographic realism
VN_PROVINCES = [
    "Hà Nội", "TP.HCM", "Đà Nẵng", "Hải Phòng", "Cần Thơ",
    "An Giang", "Bình Dương", "Đồng Nai", "Khánh Hòa", "Lâm Đồng",
    "Nghệ An", "Thanh Hóa", "Quảng Ninh", "Tiền Giang", "Bà Rịa - Vũng Tàu",
    "Hà Tĩnh", "Thừa Thiên Huế", "Quảng Nam", "Quảng Ngãi", "Bình Định",
    "Phú Yên", "Ninh Thuận", "Bình Thuận", "Kon Tum", "Gia Lai",
    "Đắk Lắk", "Đắk Nông", "Lào Cai", "Yên Bái", "Hòa Bình",
]

VN_DISTRICTS = [
    "Quận 1", "Quận 7", "Hoàn Kiếm", "Đống Đa", "Hải Châu",
    "Ngũ Hành Sơn", "Liên Chiểu", "Nha Trang", "Cam Ranh", "Biên Hòa",
    "Thủ Đức", "Bình Thạnh", "Tân Bình", "Gò Vấp", "Củ Chi",
    "Long Xuyên", "Châu Đốc", "Rạch Giá", "Cà Mau", "Bạc Liêu",
]

# Technology distribution: 4G-heavy to match real-world 2026 mix
TECHNOLOGIES = ["3G", "4G", "4G", "4G", "5G", "5G"]

# Fixed set of Vietnamese telecom operators
OPERATOR_SEEDS = [
    ("Viettel Telecom",   "VIETTEL"),
    ("Mobifone",          "MOBI"),
    ("Vinaphone",         "VINA"),
    ("Vietnamobile",      "VNMOBILE"),
    ("Gmobile",           "GMOBILE"),
    ("Reddi Network",     "REDDI"),
]

OPERATOR_NAME_BY_CODE = {code: name for name, code in OPERATOR_SEEDS}


# ---------------------------------------------------------------------------
# 8. Seed functions
# ---------------------------------------------------------------------------
def refresh_caches() -> None:
    global existing_operator_ids, existing_stations
    cur.execute("SELECT id FROM operators WHERE is_deleted = 0 ORDER BY id")
    existing_operator_ids = [row["id"] for row in cur.fetchall()]
    cur.execute(
        """
        SELECT s.id, s.station_code, s.operator_id
        FROM stations s
        INNER JOIN operators o ON o.id = s.operator_id
        WHERE s.is_deleted = 0
          AND o.is_deleted = 0
        ORDER BY s.id
        """
    )
    existing_stations = cur.fetchall()


def seed_operators() -> None:
    """Insert the fixed operator list."""
    for name, code in OPERATOR_SEEDS:
        try:
            cur.execute(
                """
                INSERT IGNORE INTO operators (name, code, created_at, is_deleted)
                VALUES (%s, %s, %s, 0)
                """,
                (name, code, FIXED_CREATED_AT),
            )
        except Exception as err:
            print(f"[WARN] operator seed error: {err}")

    conn.commit()
    refresh_caches()
    print(f"Seeded {len(existing_operator_ids)} operators.")


def seed_stations(n: int = 300) -> None:
    """Seed n stations across all operators, with dirty data injection."""
    if not existing_operator_ids:
        raise RuntimeError("seed_operators() must be called before seed_stations()")

    inserted = 0
    for _ in range(n):
        province     = random.choice(VN_PROVINCES)
        district     = random.choice(VN_DISTRICTS)
        # Vietnam bounding box: lat 8.18–23.39, lon 102.14–109.46
        lat          = round(random.uniform(8.18, 23.39), 6)
        lon          = round(random.uniform(102.14, 109.46), 6)
        install_date = (
            datetime(2018, 1, 1) + timedelta(days=random.randint(0, 2000))
        ).date()
        operator_id  = random.choice(existing_operator_ids)
        station_code = f"ST{uuid.uuid4().hex[:8].upper()}"

        province, district, lat, lon, install_date, dirty_case = maybe_corrupt_station_payload(
            province, district, lat, lon, install_date
        )

        savepoint_name = new_savepoint("sp_sta")
        cur.execute(f"SAVEPOINT {savepoint_name}")
        try:
            cur.execute(
                """
                INSERT INTO stations
                    (station_code, operator_id, province, district, latitude, longitude, install_date, status, created_at, is_deleted)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'active', %s, 0)
                """,
                (station_code, operator_id, province, district, lat, lon, install_date, FIXED_CREATED_AT),
            )
            sta_id = cur.lastrowid
            existing_stations.append({"id": sta_id, "station_code": station_code, "operator_id": operator_id})
            if dirty_case:
                dirty_stats["stations"] += 1
            inserted += 1
        except Exception as err:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            dirty_stats["failed_rows"] += 1
            log_row_error("stations", err)
        finally:
            safe_release_savepoint(savepoint_name)

    conn.commit()
    print(f"Seeded {inserted}/{n} stations.")


def mutate_operators(action: str) -> int:
    if action == "update":
        row_count = random.randint(1, min(2, MAX_MUTATION_ROWS_PER_TABLE))
        cur.execute(
            f"""
            SELECT id, code
            FROM operators
            WHERE is_deleted = 0
            ORDER BY RAND()
            LIMIT {row_count}
            """
        )
        rows = cur.fetchall()
        for row in rows:
            base_name = OPERATOR_NAME_BY_CODE.get(row["code"], row["code"])
            new_name = f"{base_name} {random.choice(['Consumer', 'Enterprise', 'Regional'])}"
            cur.execute(
                "UPDATE operators SET name = %s WHERE id = %s",
                (new_name, row["id"]),
            )
        mutation_stats["operators_updated"] += len(rows)
        return len(rows)

    cur.execute("SELECT COUNT(*) AS cnt FROM operators WHERE is_deleted = 0")
    operator_count = cur.fetchone()["cnt"]
    if operator_count <= 2:
        return 0

    cur.execute(
        """
        SELECT id
        FROM operators
        WHERE is_deleted = 0
        ORDER BY RAND()
        LIMIT 1
        """
    )
    target = cur.fetchone()
    if not target:
        return 0

    cur.execute(
        """
        SELECT id
        FROM operators
                WHERE id <> %s
                    AND is_deleted = 0
        ORDER BY RAND()
        LIMIT 1
        """,
        (target["id"],),
    )
    replacement = cur.fetchone()
    if not replacement:
        return 0

    cur.execute(
        "UPDATE stations SET operator_id = %s WHERE operator_id = %s AND is_deleted = 0",
        (replacement["id"], target["id"]),
    )
    cur.execute(
        """
        UPDATE operators
        SET is_deleted = 1,
                        deleted_at = %s
        WHERE id = %s
          AND is_deleted = 0
        """,
        (current_timestamp(), target["id"]),
    )
    deleted = cur.rowcount
    mutation_stats["operators_deleted"] += deleted
    return deleted


def mutate_stations(action: str) -> int:
    if action == "update":
        row_count = random.randint(1, MAX_MUTATION_ROWS_PER_TABLE)
        cur.execute(
            f"""
            SELECT id
            FROM stations
                        WHERE is_deleted = 0
            ORDER BY RAND()
            LIMIT {row_count}
            """
        )
        rows = cur.fetchall()
        for row in rows:
            province = random.choice(VN_PROVINCES)
            district = random.choice(VN_DISTRICTS)
            lat = round(random.uniform(8.18, 23.39), 6)
            lon = round(random.uniform(102.14, 109.46), 6)
            status = random.choice(["active", "inactive"])
            operator_id = random.choice(existing_operator_ids)
            cur.execute(
                """
                UPDATE stations
                SET province = %s,
                    district = %s,
                    latitude = %s,
                    longitude = %s,
                    status = %s,
                    operator_id = %s
                WHERE id = %s
                """,
                (province, district, lat, lon, status, operator_id, row["id"]),
            )
        mutation_stats["stations_updated"] += len(rows)
        return len(rows)

    cur.execute("SELECT COUNT(*) AS cnt FROM stations WHERE is_deleted = 0")
    station_count = cur.fetchone()["cnt"]
    delete_limit = min(MAX_MUTATION_ROWS_PER_TABLE, max(0, station_count - 10))
    if delete_limit <= 0:
        return 0

    row_count = random.randint(1, delete_limit)
    cur.execute(
        f"""
        SELECT id
        FROM stations
                WHERE is_deleted = 0
        ORDER BY RAND()
        LIMIT {row_count}
        """
    )
    rows = cur.fetchall()
    deleted = 0
    for row in rows:
        deleted_at = current_timestamp()
        cur.execute(
            """
            UPDATE station_traffic
            SET is_deleted = 1,
                                deleted_at = %s
            WHERE station_id = %s
              AND is_deleted = 0
            """,
            (deleted_at, row["id"]),
        )
        cur.execute(
            """
            UPDATE stations
            SET is_deleted = 1,
                                deleted_at = %s,
                status = 'inactive'
            WHERE id = %s
              AND is_deleted = 0
            """,
            (deleted_at, row["id"]),
        )
        deleted += cur.rowcount
    mutation_stats["stations_deleted"] += deleted
    return deleted


def mutate_station_traffic(action: str) -> int:
    if action == "update":
        row_count = random.randint(1, MAX_MUTATION_ROWS_PER_TABLE)
        cur.execute(
            f"""
            SELECT id
            FROM station_traffic
            WHERE is_deleted = 0
            ORDER BY RAND()
            LIMIT {row_count}
            """
        )
        rows = cur.fetchall()
        for row in rows:
            cur.execute(
                """
                UPDATE station_traffic
                SET technology = %s,
                    traffic_gb = %s,
                    user_count = %s,
                    event_time = %s
                WHERE id = %s
                """,
                (
                    random.choice(TECHNOLOGIES),
                    round(random.uniform(0.5, 120.0), 4),
                    random.randint(5, 8000),
                    random_event_time(),
                    row["id"],
                ),
            )
        mutation_stats["traffic_updated"] += len(rows)
        return len(rows)

    cur.execute("SELECT COUNT(*) AS cnt FROM station_traffic WHERE is_deleted = 0")
    traffic_count = cur.fetchone()["cnt"]
    delete_limit = min(MAX_MUTATION_ROWS_PER_TABLE, max(0, traffic_count - 100))
    if delete_limit <= 0:
        return 0

    row_count = random.randint(1, delete_limit)
    cur.execute(
        f"""
        SELECT id
        FROM station_traffic
        WHERE is_deleted = 0
        ORDER BY RAND()
        LIMIT {row_count}
        """
    )
    rows = cur.fetchall()
    deleted = 0
    for row in rows:
        cur.execute(
            """
            UPDATE station_traffic
            SET is_deleted = 1,
                                deleted_at = %s
            WHERE id = %s
              AND is_deleted = 0
            """,
            (current_timestamp(), row["id"]),
        )
        deleted += cur.rowcount
    mutation_stats["traffic_deleted"] += deleted
    return deleted


def apply_random_mutations() -> None:
    if not ENABLE_RANDOM_MUTATIONS:
        return

    planned_tables = random.sample(
        ["operators", "stations", "station_traffic"],
        k=random.randint(1, 3),
    )

    print(f"\nApplying random mutations on: {', '.join(planned_tables)}")
    for table_name in planned_tables:
        savepoint_name = new_savepoint(f"sp_mut_{table_name}")
        cur.execute(f"SAVEPOINT {savepoint_name}")
        try:
            action = random.choice(["update", "soft_delete"])
            if table_name == "operators":
                changed = mutate_operators(action)
            elif table_name == "stations":
                changed = mutate_stations(action)
            else:
                changed = mutate_station_traffic(action)

            if changed == 0 and action == "soft_delete":
                if table_name == "operators":
                    changed = mutate_operators("update")
                    action = "update"
                elif table_name == "stations":
                    changed = mutate_stations("update")
                    action = "update"
                else:
                    changed = mutate_station_traffic("update")
                    action = "update"

            if changed > 0:
                refresh_caches()
            print(f"Mutation {table_name}: {action} {changed} row(s)")
        except Exception as err:
            cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            log_row_error(f"mutation:{table_name}", err)
        finally:
            safe_release_savepoint(savepoint_name)

    conn.commit()
    refresh_caches()


# ---------------------------------------------------------------------------
# 9. Traffic insert
# ---------------------------------------------------------------------------
def insert_traffic_record() -> bool:
    """Insert one station_traffic row with possible dirty-data corruption."""
    if not existing_stations:
        return False

    station    = random.choice(existing_stations)
    tech       = random.choice(TECHNOLOGIES)
    # Typical hourly-ish reading: 0.5–80 GB, 10–5000 active users
    traffic_gb = round(random.uniform(0.5, 80.0), 4)
    user_count = random.randint(10, 5000)
    event_time = random_event_time()

    tech, traffic_gb, user_count, event_time, dirty_case = maybe_corrupt_traffic_payload(
        tech, traffic_gb, user_count, event_time
    )

    savepoint_name = new_savepoint("sp_traf")
    cur.execute(f"SAVEPOINT {savepoint_name}")
    try:
        cur.execute(
            """
            INSERT INTO station_traffic
                (station_id, technology, traffic_gb, user_count, event_time, is_deleted)
            VALUES (%s, %s, %s, %s, %s, 0)
            """,
            (station["id"], tech, traffic_gb, user_count, event_time),
        )
        if dirty_case:
            dirty_stats["traffic"] += 1
        return True
    except Exception as err:
        cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
        dirty_stats["failed_rows"] += 1
        log_row_error("station_traffic", err)
        return False
    finally:
        safe_release_savepoint(savepoint_name)


# ---------------------------------------------------------------------------
# 10. Main execution
# ---------------------------------------------------------------------------
# seed_operators()
# seed_stations(300)

refresh_caches()

TOTAL_TRAFFIC = 100_000
BATCH_SIZE    = 2_000
successful_inserts = 0

print(f"\nStarting to insert {TOTAL_TRAFFIC:,} traffic records...")
for batch_num in range(TOTAL_TRAFFIC // BATCH_SIZE):
    try:
        batch_success = 0
        for _ in range(BATCH_SIZE):
            if insert_traffic_record():
                batch_success += 1

        conn.commit()
        successful_inserts += batch_success
        print(
            f"Committed batch {batch_num + 1}/{TOTAL_TRAFFIC // BATCH_SIZE} "
            f"| inserted: {batch_success}/{BATCH_SIZE}"
        )
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] batch {batch_num}: {e}")

print(f"\nInserted {successful_inserts:,}/{TOTAL_TRAFFIC:,} traffic records successfully.")
apply_random_mutations()
print_dirty_summary()
print_mutation_summary()
