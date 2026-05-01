from __future__ import annotations

import csv
import math
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

STOP_CSV = PROJECT_ROOT / "tools" / "stop_level.csv"
DISPATCH_SCOPE = "weekday_city_dual_w1"
CALC_VERSION = "w1_elapsed_p10_p90_v2"
W1_START_HHMM = "19:30"


def mysql_conn():
    return pymysql.connect(
        host=settings.MYSQL_HOST,
        port=int(settings.MYSQL_PORT),
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
        database=settings.MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def hhmm_to_min(hhmm: str) -> int:
    h, m = str(hhmm).strip().split(":")
    return int(h) * 60 + int(m)


def min_to_hhmm(mins: float) -> str:
    m = int(round(mins)) % (24 * 60)
    return f"{m // 60:02d}:{m % 60:02d}"


def percentile(sorted_vals, p):
    if not sorted_vals:
        return None
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    k = (len(sorted_vals) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(sorted_vals[int(k)])
    d0 = sorted_vals[f] * (c - k)
    d1 = sorted_vals[c] * (k - f)
    return float(d0 + d1)


def load_elapsed_samples():
    base_start_min = hhmm_to_min(W1_START_HHMM)
    per_store = {}
    source_date_start = None
    source_date_end = None
    total_samples = 0

    with STOP_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = str((row or {}).get("store_id") or "").strip()
            sname = str((row or {}).get("store_name") or "").strip()
            d = str((row or {}).get("delivery_date") or "").strip()
            arr = str((row or {}).get("computed_arrival_time") or "").strip()
            wave_tag = str((row or {}).get("wave_tag") or "").strip().upper()
            if not sid or not d:
                continue

            bucket = per_store.get(sid)
            if bucket is None:
                bucket = {
                    "store_id": sid,
                    "store_name": sname,
                    "raw_count": 0,
                    "valid_count": 0,
                    "samples": [],
                    "min_date": d,
                    "max_date": d,
                }
                per_store[sid] = bucket

            bucket["raw_count"] += 1
            if d < bucket["min_date"]:
                bucket["min_date"] = d
            if d > bucket["max_date"]:
                bucket["max_date"] = d

            if source_date_start is None or d < source_date_start:
                source_date_start = d
            if source_date_end is None or d > source_date_end:
                source_date_end = d
            total_samples += 1

            # 过滤规则：
            # 1) is_weekday_city_dual_w1 = true  -> 当前口径用 wave_tag=W1 且非周日
            # 2) computed_arrival_time 非空
            # 3) 0 < elapsed_minutes < 600
            # 4) stop_level 有效记录（本脚本以 computed_arrival_time 非空作为有效记录）
            is_sunday = datetime.strptime(d, "%Y-%m-%d").weekday() == 6
            if wave_tag != "W1" or is_sunday:
                continue
            if not arr:
                continue

            arr_dt = datetime.strptime(arr, "%Y-%m-%d %H:%M")
            arr_hhmm = arr_dt.strftime("%H:%M")
            arr_min_of_day = hhmm_to_min(arr_hhmm)
            base_dt = datetime.strptime(f"{d} {W1_START_HHMM}", "%Y-%m-%d %H:%M")

            if arr_min_of_day >= base_start_min:
                normalized_arr_dt = datetime.strptime(f"{d} {arr_hhmm}", "%Y-%m-%d %H:%M")
            elif arr_min_of_day <= 8 * 60:
                normalized_arr_dt = datetime.strptime(f"{d} {arr_hhmm}", "%Y-%m-%d %H:%M") + timedelta(days=1)
            else:
                continue

            elapsed = int((normalized_arr_dt - base_dt).total_seconds() // 60)
            if not (0 < elapsed < 600):
                continue

            bucket["samples"].append(elapsed)
            bucket["valid_count"] += 1

    return per_store, total_samples, source_date_start, source_date_end, base_start_min


def ensure_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rengong_store_arrival_windows (
          id BIGINT NOT NULL AUTO_INCREMENT,
          store_id VARCHAR(64) NOT NULL,
          store_name VARCHAR(255) NULL,
          dispatch_scope VARCHAR(64) NOT NULL,
          sample_count INT NOT NULL DEFAULT 0,
          raw_sample_count INT NOT NULL DEFAULT 0,
          valid_sample_count INT NOT NULL DEFAULT 0,
          earliest_arrival_time VARCHAR(8) NOT NULL,
          latest_arrival_time VARCHAR(8) NOT NULL,
          avg_arrival_time VARCHAR(8) NOT NULL,
          early_allowed_minutes DECIMAL(10,2) NOT NULL DEFAULT 0,
          late_allowed_minutes DECIMAL(10,2) NOT NULL DEFAULT 0,
          source_date_start DATE NULL,
          source_date_end DATE NULL,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          UNIQUE KEY uk_scope_version_store (dispatch_scope, calc_version, store_id),
          KEY idx_scope_version (dispatch_scope, calc_version)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cursor.execute("SHOW COLUMNS FROM rengong_store_arrival_windows")
    cols = {str((r or {}).get("Field") or "").strip().lower() for r in (cursor.fetchall() or [])}
    if "raw_sample_count" not in cols:
        cursor.execute(
            "ALTER TABLE rengong_store_arrival_windows ADD COLUMN raw_sample_count INT NOT NULL DEFAULT 0 AFTER sample_count"
        )
    if "valid_sample_count" not in cols:
        cursor.execute(
            "ALTER TABLE rengong_store_arrival_windows ADD COLUMN valid_sample_count INT NOT NULL DEFAULT 0 AFTER raw_sample_count"
        )


def main():
    if not STOP_CSV.exists():
        raise FileNotFoundError(f"missing_csv:{STOP_CSV}")

    per_store, total_samples, src_start, src_end, base_start_min = load_elapsed_samples()
    rows = []
    filtered_stores = []
    for sid, item in per_store.items():
        raw_count = int(item.get("raw_count") or 0)
        valid_count = int(item.get("valid_count") or 0)
        if valid_count < 15:
            filtered_stores.append((sid, str(item.get("store_name") or ""), raw_count, valid_count))
            continue

        vals = sorted(item["samples"])
        n = len(vals)
        avg = sum(vals) / n
        p10 = percentile(vals, 0.10)
        p90 = percentile(vals, 0.90)
        early_allowed = round(avg - p10, 2)
        late_allowed = round(p90 - avg, 2)
        rows.append(
            {
                "store_id": sid,
                "store_name": item["store_name"],
                "sample_count": n,
                "raw_sample_count": raw_count,
                "valid_sample_count": valid_count,
                "earliest_arrival_time": min_to_hhmm(base_start_min + min(vals)),
                "latest_arrival_time": min_to_hhmm(base_start_min + max(vals)),
                "avg_arrival_time": min_to_hhmm(base_start_min + avg),
                "early_allowed_minutes": early_allowed,
                "late_allowed_minutes": late_allowed,
            }
        )

    with mysql_conn() as conn:
        with conn.cursor() as c:
            ensure_table(c)
            c.execute(
                """
                DELETE FROM rengong_store_arrival_windows
                WHERE dispatch_scope=%s AND calc_version=%s
                """,
                (DISPATCH_SCOPE, CALC_VERSION),
            )

            sql = """
                INSERT INTO rengong_store_arrival_windows (
                  store_id, store_name, dispatch_scope, sample_count,
                  raw_sample_count, valid_sample_count,
                  earliest_arrival_time, latest_arrival_time, avg_arrival_time,
                  early_allowed_minutes, late_allowed_minutes,
                  source_date_start, source_date_end, calc_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """
            for r in rows:
                c.execute(
                    sql,
                    (
                        r["store_id"],
                        r["store_name"],
                        DISPATCH_SCOPE,
                        int(r["sample_count"]),
                        int(r["raw_sample_count"]),
                        int(r["valid_sample_count"]),
                        r["earliest_arrival_time"],
                        r["latest_arrival_time"],
                        r["avg_arrival_time"],
                        r["early_allowed_minutes"],
                        r["late_allowed_minutes"],
                        src_start,
                        src_end,
                        CALC_VERSION,
                    ),
                )

    print(f"written_store_count={len(rows)}")
    print(f"filtered_store_count={len(filtered_stores)}")
    print(f"total_samples={int(total_samples)}")
    print(f"source_date_start={src_start or ''}")
    print(f"source_date_end={src_end or ''}")
    print(f"dispatch_scope={DISPATCH_SCOPE}")
    print(f"calc_version={CALC_VERSION}")
    print("valid_sample_count_by_store:")
    for sid, item in sorted(per_store.items(), key=lambda x: (int((x[1] or {}).get("valid_count") or 0), str(x[0]))):
        print(
            f"{sid}\t{str((item or {}).get('store_name') or '')}\traw={int((item or {}).get('raw_count') or 0)}\tvalid={int((item or {}).get('valid_count') or 0)}"
        )


if __name__ == "__main__":
    main()
