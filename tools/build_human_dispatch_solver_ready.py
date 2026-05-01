# -*- coding: utf-8 -*-
"""把五个月人工调度明细转换成求解可直接使用的新表。

说明：
1. 只读来源表：human_dispatch_routes / store_wave_timing_resolved / store_wave_load_resolved
2. 只写目标表：human_dispatch_solver_ready
3. 保留原始货量字段，同时补齐求解器识别字段与折算后的货量字段
"""

import os
import re
import sys
from collections import defaultdict

import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings  # noqa


TARGET_TABLE = "human_dispatch_solver_ready"

CREATE_SQL = f"""
CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  delivery_date DATE NOT NULL,
  shop_code VARCHAR(32) NOT NULL,
  store_name VARCHAR(255) NULL,

  source_record_count INT NOT NULL DEFAULT 0,
  source_import_batch_ids TEXT NULL,
  source_vehicle_ids TEXT NULL,
  source_route_ids TEXT NULL,
  source_route_names TEXT NULL,
  source_stop_sequences TEXT NULL,
  source_companies TEXT NULL,
  source_drivers TEXT NULL,

  original_ambient_turnover_boxes DECIMAL(18,6) NOT NULL DEFAULT 0,
  original_ambient_cartons DECIMAL(18,6) NOT NULL DEFAULT 0,
  original_ambient_full_cases DECIMAL(18,6) NOT NULL DEFAULT 0,
  original_frozen_bags DECIMAL(18,6) NOT NULL DEFAULT 0,
  original_frozen_cartons DECIMAL(18,6) NOT NULL DEFAULT 0,
  original_cold_bins DECIMAL(18,6) NOT NULL DEFAULT 0,
  original_cold_cartons DECIMAL(18,6) NOT NULL DEFAULT 0,
  original_load DECIMAL(18,6) NOT NULL DEFAULT 0,

  rpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
  rcase DECIMAL(18,6) NOT NULL DEFAULT 0,
  bpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
  bpaper DECIMAL(18,6) NOT NULL DEFAULT 0,
  apcs DECIMAL(18,6) NOT NULL DEFAULT 0,
  apaper DECIMAL(18,6) NOT NULL DEFAULT 0,
  rpaper DECIMAL(18,6) NOT NULL DEFAULT 0,

  wave_belongs VARCHAR(32) NULL,
  first_wave_time VARCHAR(16) NULL,
  second_wave_time VARCHAR(16) NULL,
  arrival_time_w3 VARCHAR(16) NULL,
  arrival_time_w4 VARCHAR(16) NULL,

  wave1_load DECIMAL(18,6) NOT NULL DEFAULT 0,
  wave2_load DECIMAL(18,6) NOT NULL DEFAULT 0,
  wave3_load DECIMAL(18,6) NOT NULL DEFAULT 0,
  wave4_load DECIMAL(18,6) NOT NULL DEFAULT 0,
  total_resolved_load DECIMAL(18,6) NOT NULL DEFAULT 0,
  boxes DECIMAL(18,6) NOT NULL DEFAULT 0,
  totalLoad DECIMAL(18,6) NOT NULL DEFAULT 0,
  resolvedTotalLoad DECIMAL(18,6) NOT NULL DEFAULT 0,

  timing_source VARCHAR(32) NULL,
  solver_ready_flag TINYINT(1) NOT NULL DEFAULT 0,
  missing_fields VARCHAR(255) NULL,
  conversion_rule_version VARCHAR(32) NOT NULL DEFAULT 'human_dispatch_v1',
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

  UNIQUE KEY uk_hdsr_day_shop (delivery_date, shop_code),
  KEY idx_hdsr_date_wave (delivery_date, wave_belongs),
  KEY idx_hdsr_date_ready (delivery_date, solver_ready_flag),
  KEY idx_hdsr_shop (shop_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""


INSERT_SQL = f"""
INSERT INTO {TARGET_TABLE} (
  delivery_date, shop_code, store_name,
  source_record_count, source_import_batch_ids, source_vehicle_ids, source_route_ids, source_route_names,
  source_stop_sequences, source_companies, source_drivers,
  original_ambient_turnover_boxes, original_ambient_cartons, original_ambient_full_cases,
  original_frozen_bags, original_frozen_cartons, original_cold_bins, original_cold_cartons, original_load,
  rpcs, rcase, bpcs, bpaper, apcs, apaper, rpaper,
  wave_belongs, first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4,
  wave1_load, wave2_load, wave3_load, wave4_load, total_resolved_load, boxes, totalLoad, resolvedTotalLoad,
  timing_source, solver_ready_flag, missing_fields, conversion_rule_version
) VALUES (
  %(delivery_date)s, %(shop_code)s, %(store_name)s,
  %(source_record_count)s, %(source_import_batch_ids)s, %(source_vehicle_ids)s, %(source_route_ids)s, %(source_route_names)s,
  %(source_stop_sequences)s, %(source_companies)s, %(source_drivers)s,
  %(original_ambient_turnover_boxes)s, %(original_ambient_cartons)s, %(original_ambient_full_cases)s,
  %(original_frozen_bags)s, %(original_frozen_cartons)s, %(original_cold_bins)s, %(original_cold_cartons)s, %(original_load)s,
  %(rpcs)s, %(rcase)s, %(bpcs)s, %(bpaper)s, %(apcs)s, %(apaper)s, %(rpaper)s,
  %(wave_belongs)s, %(first_wave_time)s, %(second_wave_time)s, %(arrival_time_w3)s, %(arrival_time_w4)s,
  %(wave1_load)s, %(wave2_load)s, %(wave3_load)s, %(wave4_load)s, %(total_resolved_load)s, %(boxes)s, %(totalLoad)s, %(resolvedTotalLoad)s,
  %(timing_source)s, %(solver_ready_flag)s, %(missing_fields)s, %(conversion_rule_version)s
)
ON DUPLICATE KEY UPDATE
  store_name=VALUES(store_name),
  source_record_count=VALUES(source_record_count),
  source_import_batch_ids=VALUES(source_import_batch_ids),
  source_vehicle_ids=VALUES(source_vehicle_ids),
  source_route_ids=VALUES(source_route_ids),
  source_route_names=VALUES(source_route_names),
  source_stop_sequences=VALUES(source_stop_sequences),
  source_companies=VALUES(source_companies),
  source_drivers=VALUES(source_drivers),
  original_ambient_turnover_boxes=VALUES(original_ambient_turnover_boxes),
  original_ambient_cartons=VALUES(original_ambient_cartons),
  original_ambient_full_cases=VALUES(original_ambient_full_cases),
  original_frozen_bags=VALUES(original_frozen_bags),
  original_frozen_cartons=VALUES(original_frozen_cartons),
  original_cold_bins=VALUES(original_cold_bins),
  original_cold_cartons=VALUES(original_cold_cartons),
  original_load=VALUES(original_load),
  rpcs=VALUES(rpcs),
  rcase=VALUES(rcase),
  bpcs=VALUES(bpcs),
  bpaper=VALUES(bpaper),
  apcs=VALUES(apcs),
  apaper=VALUES(apaper),
  rpaper=VALUES(rpaper),
  wave_belongs=VALUES(wave_belongs),
  first_wave_time=VALUES(first_wave_time),
  second_wave_time=VALUES(second_wave_time),
  arrival_time_w3=VALUES(arrival_time_w3),
  arrival_time_w4=VALUES(arrival_time_w4),
  wave1_load=VALUES(wave1_load),
  wave2_load=VALUES(wave2_load),
  wave3_load=VALUES(wave3_load),
  wave4_load=VALUES(wave4_load),
  total_resolved_load=VALUES(total_resolved_load),
  boxes=VALUES(boxes),
  totalLoad=VALUES(totalLoad),
  resolvedTotalLoad=VALUES(resolvedTotalLoad),
  timing_source=VALUES(timing_source),
  solver_ready_flag=VALUES(solver_ready_flag),
  missing_fields=VALUES(missing_fields),
  conversion_rule_version=VALUES(conversion_rule_version);
"""


def mysql_connection():
    return pymysql.connect(
        host=settings.MYSQL_HOST,
        port=int(settings.MYSQL_PORT),
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
        database=settings.MYSQL_DATABASE,
        charset="utf8mb4",
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _safe_float(value):
    try:
        if value is None:
            return 0.0
        return float(value)
    except Exception:
        return 0.0


def _normalize_wave_belongs(value):
    text = str(value or "").strip()
    if not text:
        return ""
    return re.sub(r"[，、\s]+", ",", text).replace(",,", ",").strip(",")


def _compute_resolved_loads(wave_belongs, rpcs, rcase, bpcs, bpaper, apcs, apaper, rpaper):
    norm_belongs = _normalize_wave_belongs(wave_belongs)
    base_w1 = (rpcs / 207.0) + (rcase / 380.0) + (bpcs / 120.0) + (bpaper / 380.0) + (rpaper / 380.0)
    base_w2 = (apcs / 350.0) + (apaper / 380.0)
    total_full = base_w1 + base_w2

    has_1 = bool(re.search(r"(^|,)\s*1\s*(,|$)", norm_belongs))
    has_2 = bool(re.search(r"(^|,)\s*2\s*(,|$)", norm_belongs))
    has_3 = bool(re.search(r"(^|,)\s*3\s*(,|$)", norm_belongs))
    has_4 = bool(re.search(r"(^|,)\s*4\s*(,|$)", norm_belongs))

    if norm_belongs == "2":
        wave1_load = 0.0
        wave2_load = total_full
        wave3_load = 0.0
        wave4_load = 0.0
    elif has_3 or has_4:
        wave1_load = 0.0
        wave2_load = 0.0
        wave3_load = total_full if has_3 else 0.0
        wave4_load = total_full if has_4 else 0.0
    elif has_1 or has_2:
        wave1_load = base_w1
        wave2_load = base_w2
        wave3_load = 0.0
        wave4_load = 0.0
    else:
        wave1_load = 0.0
        wave2_load = 0.0
        wave3_load = 0.0
        wave4_load = 0.0

    total_resolved_load = wave1_load + wave2_load + wave3_load + wave4_load
    return {
        "wave1_load": round(wave1_load, 6),
        "wave2_load": round(wave2_load, 6),
        "wave3_load": round(wave3_load, 6),
        "wave4_load": round(wave4_load, 6),
        "total_resolved_load": round(total_resolved_load, 6),
    }


def _load_timing_maps(cursor):
    timing_map = {}
    cursor.execute(
        """
        SELECT
          shop_code,
          wave_belongs,
          first_wave_time,
          second_wave_time,
          arrival_time_w3,
          arrival_time_w4
        FROM store_wave_timing_resolved
        """
    )
    for row in cursor.fetchall() or []:
        code = str(row.get("shop_code") or "").strip()
        if not code:
            continue
        timing_map[code] = {
            "wave_belongs": str(row.get("wave_belongs") or "").strip(),
            "first_wave_time": str(row.get("first_wave_time") or "").strip() or None,
            "second_wave_time": str(row.get("second_wave_time") or "").strip() or None,
            "arrival_time_w3": str(row.get("arrival_time_w3") or "").strip() or None,
            "arrival_time_w4": str(row.get("arrival_time_w4") or "").strip() or None,
            "timing_source": "store_wave_timing_resolved",
        }

    cursor.execute(
        """
        SELECT
          shop_code,
          wave_belongs,
          first_wave_time,
          second_wave_time,
          arrival_time_w3,
          arrival_time_w4
        FROM store_wave_load_resolved
        """
    )
    load_map = {}
    for row in cursor.fetchall() or []:
        code = str(row.get("shop_code") or "").strip()
        if not code:
            continue
        load_map[code] = {
            "wave_belongs": str(row.get("wave_belongs") or "").strip(),
            "first_wave_time": str(row.get("first_wave_time") or "").strip() or None,
            "second_wave_time": str(row.get("second_wave_time") or "").strip() or None,
            "arrival_time_w3": str(row.get("arrival_time_w3") or "").strip() or None,
            "arrival_time_w4": str(row.get("arrival_time_w4") or "").strip() or None,
            "timing_source": "store_wave_load_resolved",
        }
    return timing_map, load_map


def _fetch_dedup_human_rows(cursor):
    cursor.execute(
        """
        SELECT h.*
        FROM human_dispatch_routes h
        INNER JOIN (
          SELECT MAX(id) AS keep_id
          FROM human_dispatch_routes
          GROUP BY delivery_date, vehicle_id, route_id, stop_sequence, store_id
        ) d ON h.id = d.keep_id
        ORDER BY h.delivery_date, h.store_id, h.stop_sequence, h.id
        """
    )
    return cursor.fetchall() or []


def _build_rows(human_rows, timing_map, load_map):
    grouped = {}
    for row in human_rows:
        delivery_date = row.get("delivery_date")
        shop_code = str(row.get("store_id") or "").strip()
        if not delivery_date or not shop_code:
            continue
        key = (delivery_date, shop_code)
        item = grouped.get(key)
        if item is None:
            item = {
                "delivery_date": delivery_date,
                "shop_code": shop_code,
                "store_name": str(row.get("store_name") or "").strip() or None,
                "source_record_count": 0,
                "source_import_batch_ids": set(),
                "source_vehicle_ids": set(),
                "source_route_ids": set(),
                "source_route_names": set(),
                "source_stop_sequences": set(),
                "source_companies": set(),
                "source_drivers": set(),
                "original_ambient_turnover_boxes": 0.0,
                "original_ambient_cartons": 0.0,
                "original_ambient_full_cases": 0.0,
                "original_frozen_bags": 0.0,
                "original_frozen_cartons": 0.0,
                "original_cold_bins": 0.0,
                "original_cold_cartons": 0.0,
            }
            grouped[key] = item

        item["source_record_count"] += 1
        if row.get("import_batch_id") is not None:
            item["source_import_batch_ids"].add(str(row.get("import_batch_id")))
        if row.get("vehicle_id"):
            item["source_vehicle_ids"].add(str(row.get("vehicle_id")))
        if row.get("route_id") is not None:
            item["source_route_ids"].add(str(row.get("route_id")))
        if row.get("route_name"):
            item["source_route_names"].add(str(row.get("route_name")))
        if row.get("stop_sequence") is not None:
            item["source_stop_sequences"].add(str(row.get("stop_sequence")))
        if row.get("company_name"):
            item["source_companies"].add(str(row.get("company_name")))
        if row.get("driver_name"):
            item["source_drivers"].add(str(row.get("driver_name")))

        item["original_ambient_turnover_boxes"] += _safe_float(row.get("ambient_turnover_boxes"))
        item["original_ambient_cartons"] += _safe_float(row.get("ambient_cartons"))
        item["original_ambient_full_cases"] += _safe_float(row.get("ambient_full_cases"))
        item["original_frozen_bags"] += _safe_float(row.get("frozen_bags"))
        item["original_frozen_cartons"] += _safe_float(row.get("frozen_cartons"))
        item["original_cold_bins"] += _safe_float(row.get("cold_bins"))
        item["original_cold_cartons"] += _safe_float(row.get("cold_cartons"))

    result_rows = []
    for (_, shop_code), item in grouped.items():
        timing = timing_map.get(shop_code) or load_map.get(shop_code) or {}
        wave_belongs = str(timing.get("wave_belongs") or "").strip() or None
        first_wave_time = timing.get("first_wave_time")
        second_wave_time = timing.get("second_wave_time")
        arrival_time_w3 = timing.get("arrival_time_w3")
        arrival_time_w4 = timing.get("arrival_time_w4")
        timing_source = str(timing.get("timing_source") or "").strip() or None

        rpcs = round(item["original_ambient_turnover_boxes"], 6)
        rcase = round(item["original_ambient_full_cases"], 6)
        bpcs = round(item["original_frozen_bags"], 6)
        bpaper = round(item["original_frozen_cartons"], 6)
        apcs = round(item["original_cold_bins"], 6)
        apaper = round(item["original_cold_cartons"], 6)
        rpaper = round(item["original_ambient_cartons"], 6)
        original_load = round(
            rpcs + rcase + bpcs + bpaper + apcs + apaper + rpaper,
            6,
        )

        resolved = _compute_resolved_loads(
            wave_belongs,
            rpcs,
            rcase,
            bpcs,
            bpaper,
            apcs,
            apaper,
            rpaper,
        )

        missing = []
        norm_belongs = _normalize_wave_belongs(wave_belongs)
        if not norm_belongs:
            missing.append("wave_belongs")
        if "1" in norm_belongs and not first_wave_time:
            missing.append("first_wave_time")
        if "2" in norm_belongs and not second_wave_time:
            missing.append("second_wave_time")
        if "3" in norm_belongs and not arrival_time_w3:
            missing.append("arrival_time_w3")
        if "4" in norm_belongs and not arrival_time_w4:
            missing.append("arrival_time_w4")

        row = {
            "delivery_date": item["delivery_date"],
            "shop_code": shop_code,
            "store_name": item["store_name"],
            "source_record_count": item["source_record_count"],
            "source_import_batch_ids": ",".join(sorted(item["source_import_batch_ids"])),
            "source_vehicle_ids": ",".join(sorted(item["source_vehicle_ids"])),
            "source_route_ids": ",".join(sorted(item["source_route_ids"])),
            "source_route_names": " | ".join(sorted(item["source_route_names"])),
            "source_stop_sequences": ",".join(sorted(item["source_stop_sequences"], key=lambda x: int(x))),
            "source_companies": " | ".join(sorted(item["source_companies"])),
            "source_drivers": " | ".join(sorted(item["source_drivers"])),
            "original_ambient_turnover_boxes": round(item["original_ambient_turnover_boxes"], 6),
            "original_ambient_cartons": round(item["original_ambient_cartons"], 6),
            "original_ambient_full_cases": round(item["original_ambient_full_cases"], 6),
            "original_frozen_bags": round(item["original_frozen_bags"], 6),
            "original_frozen_cartons": round(item["original_frozen_cartons"], 6),
            "original_cold_bins": round(item["original_cold_bins"], 6),
            "original_cold_cartons": round(item["original_cold_cartons"], 6),
            "original_load": original_load,
            "rpcs": rpcs,
            "rcase": rcase,
            "bpcs": bpcs,
            "bpaper": bpaper,
            "apcs": apcs,
            "apaper": apaper,
            "rpaper": rpaper,
            "wave_belongs": wave_belongs,
            "first_wave_time": first_wave_time,
            "second_wave_time": second_wave_time,
            "arrival_time_w3": arrival_time_w3,
            "arrival_time_w4": arrival_time_w4,
            "wave1_load": resolved["wave1_load"],
            "wave2_load": resolved["wave2_load"],
            "wave3_load": resolved["wave3_load"],
            "wave4_load": resolved["wave4_load"],
            "total_resolved_load": resolved["total_resolved_load"],
            "boxes": resolved["total_resolved_load"],
            "totalLoad": resolved["total_resolved_load"],
            "resolvedTotalLoad": resolved["total_resolved_load"],
            "timing_source": timing_source,
            "solver_ready_flag": 0 if missing else 1,
            "missing_fields": ",".join(missing) or None,
            "conversion_rule_version": "human_dispatch_v1",
        }
        result_rows.append(row)

    return result_rows


def rebuild():
    with mysql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(CREATE_SQL)
            timing_map, load_map = _load_timing_maps(cursor)
            human_rows = _fetch_dedup_human_rows(cursor)
            output_rows = _build_rows(human_rows, timing_map, load_map)
            if output_rows:
                cursor.executemany(INSERT_SQL, output_rows)
            conn.commit()

            cursor.execute(f"SELECT COUNT(*) AS cnt FROM {TARGET_TABLE}")
            total = int((cursor.fetchone() or {}).get("cnt") or 0)
            cursor.execute(
                f"""
                SELECT solver_ready_flag, COUNT(*) AS cnt
                FROM {TARGET_TABLE}
                GROUP BY solver_ready_flag
                ORDER BY solver_ready_flag DESC
                """
            )
            ready_stats = cursor.fetchall() or []
            cursor.execute(
                f"""
                SELECT
                  delivery_date, shop_code, store_name, wave_belongs,
                  original_load, total_resolved_load, boxes,
                  first_wave_time, second_wave_time, arrival_time_w3, arrival_time_w4,
                  solver_ready_flag, missing_fields
                FROM {TARGET_TABLE}
                ORDER BY delivery_date DESC, shop_code
                LIMIT 12
                """
            )
            samples = cursor.fetchall() or []

    print(f"table={TARGET_TABLE}")
    print(f"rows={total}")
    print("ready_stats=")
    for row in ready_stats:
        print(row)
    print("sample_rows=")
    for row in samples:
        print(row)


if __name__ == "__main__":
    rebuild()
