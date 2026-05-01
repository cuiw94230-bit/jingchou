from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

CALC_VERSION = "chaos_replay_task_v1"
SOURCE = "human_dispatch_routes"


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


def _to_float(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _is_sunday(date_text: str) -> bool:
    return datetime.strptime(date_text, "%Y-%m-%d").weekday() == 6


def ensure_tables(c):
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_replay_tasks (
          id BIGINT NOT NULL AUTO_INCREMENT,
          delivery_date DATE NOT NULL,
          day_type VARCHAR(32) NOT NULL,
          store_count INT NOT NULL DEFAULT 0,
          vehicle_count INT NOT NULL DEFAULT 0,
          route_count INT NOT NULL DEFAULT 0,
          total_load_w1 DECIMAL(18,6) NOT NULL DEFAULT 0,
          total_load_w2 DECIMAL(18,6) NOT NULL DEFAULT 0,
          total_load DECIMAL(18,6) NOT NULL DEFAULT 0,
          source VARCHAR(64) NOT NULL,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_calc_version (calc_version),
          KEY idx_delivery_date (delivery_date)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_replay_task_stores (
          id BIGINT NOT NULL AUTO_INCREMENT,
          delivery_date DATE NOT NULL,
          store_id VARCHAR(64) NOT NULL,
          store_name VARCHAR(255) NULL,
          sum_rpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_rcase DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_rpaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_bpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_bpaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_apcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_apaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          load_w1 DECIMAL(18,6) NOT NULL DEFAULT 0,
          load_w2 DECIMAL(18,6) NOT NULL DEFAULT 0,
          total_load DECIMAL(18,6) NOT NULL DEFAULT 0,
          original_route_ids JSON NULL,
          original_vehicle_ids JSON NULL,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_calc_version (calc_version),
          KEY idx_delivery_store (delivery_date, store_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )

    c.execute(
        """
        CREATE TABLE IF NOT EXISTS chaos_replay_task_vehicles (
          id BIGINT NOT NULL AUTO_INCREMENT,
          delivery_date DATE NOT NULL,
          vehicle_id VARCHAR(64) NOT NULL,
          driver_name VARCHAR(128) NULL,
          original_route_ids JSON NULL,
          route_count INT NOT NULL DEFAULT 0,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_calc_version (calc_version),
          KEY idx_delivery_vehicle (delivery_date, vehicle_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def main():
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT
                  delivery_date,
                  vehicle_id,
                  route_id,
                  route_name,
                  store_id,
                  store_name,
                  driver_name,
                  ambient_turnover_boxes,
                  ambient_full_cases,
                  ambient_cartons,
                  frozen_bags,
                  frozen_cartons,
                  cold_bins,
                  cold_cartons
                FROM human_dispatch_routes
                ORDER BY delivery_date, route_id
                """
            )
            rows = c.fetchall() or []

    # daily aggregators
    day_route_set = defaultdict(set)
    day_vehicle_set = defaultdict(set)
    day_store_set = defaultdict(set)

    day_load_w1 = defaultdict(float)
    day_load_w2 = defaultdict(float)

    # store-level aggregators
    store_agg = {}

    # vehicle-level aggregators
    veh_agg = {}

    date_set = set()

    for r in rows:
        d = str((r or {}).get("delivery_date") or "").strip()
        if not d:
            continue
        date_set.add(d)

        route_id = str((r or {}).get("route_id") or "").strip()
        vehicle_id = str((r or {}).get("vehicle_id") or "").strip()
        store_id = str((r or {}).get("store_id") or "").strip()
        store_name = str((r or {}).get("store_name") or "").strip()
        driver_name = str((r or {}).get("driver_name") or "").strip()

        if route_id:
            day_route_set[d].add(route_id)
        if vehicle_id:
            day_vehicle_set[d].add(vehicle_id)
        if store_id:
            day_store_set[d].add(store_id)

        rpcs = _to_float((r or {}).get("ambient_turnover_boxes"))
        rcase = _to_float((r or {}).get("ambient_full_cases"))
        rpaper = _to_float((r or {}).get("ambient_cartons"))
        bpcs = _to_float((r or {}).get("frozen_bags"))
        bpaper = _to_float((r or {}).get("frozen_cartons"))
        apcs = _to_float((r or {}).get("cold_bins"))
        apaper = _to_float((r or {}).get("cold_cartons"))

        w1 = rpcs / 207.0 + rcase / 380.0 + rpaper / 380.0 + bpcs / 120.0 + bpaper / 380.0
        w2 = apcs / 350.0 + apaper / 380.0

        day_load_w1[d] += w1
        day_load_w2[d] += w2

        if store_id:
            sk = (d, store_id)
            if sk not in store_agg:
                store_agg[sk] = {
                    "delivery_date": d,
                    "store_id": store_id,
                    "store_name": store_name,
                    "sum_rpcs": 0.0,
                    "sum_rcase": 0.0,
                    "sum_rpaper": 0.0,
                    "sum_bpcs": 0.0,
                    "sum_bpaper": 0.0,
                    "sum_apcs": 0.0,
                    "sum_apaper": 0.0,
                    "routes": set(),
                    "vehicles": set(),
                }
            a = store_agg[sk]
            if store_name and not a["store_name"]:
                a["store_name"] = store_name
            a["sum_rpcs"] += rpcs
            a["sum_rcase"] += rcase
            a["sum_rpaper"] += rpaper
            a["sum_bpcs"] += bpcs
            a["sum_bpaper"] += bpaper
            a["sum_apcs"] += apcs
            a["sum_apaper"] += apaper
            if route_id:
                a["routes"].add(route_id)
            if vehicle_id:
                a["vehicles"].add(vehicle_id)

        if vehicle_id:
            vk = (d, vehicle_id)
            if vk not in veh_agg:
                veh_agg[vk] = {
                    "delivery_date": d,
                    "vehicle_id": vehicle_id,
                    "driver_name": driver_name,
                    "routes": set(),
                }
            v = veh_agg[vk]
            if driver_name and not v["driver_name"]:
                v["driver_name"] = driver_name
            if route_id:
                v["routes"].add(route_id)

    # build rows
    task_rows = []
    for d in sorted(date_set):
        lw1 = day_load_w1[d]
        lw2 = day_load_w2[d]
        total = lw1 + lw2
        task_rows.append(
            {
                "delivery_date": d,
                "day_type": "COLD_ONLY" if _is_sunday(d) else "NORMAL",
                "store_count": len(day_store_set[d]),
                "vehicle_count": len(day_vehicle_set[d]),
                "route_count": len(day_route_set[d]),
                "total_load_w1": round(lw1, 6),
                "total_load_w2": round(lw2, 6),
                "total_load": round(total, 6),
                "source": SOURCE,
                "calc_version": CALC_VERSION,
            }
        )

    store_rows = []
    for (_d, _sid), a in sorted(store_agg.items(), key=lambda x: (x[0][0], x[0][1])):
        w1 = a["sum_rpcs"] / 207.0 + a["sum_rcase"] / 380.0 + a["sum_rpaper"] / 380.0 + a["sum_bpcs"] / 120.0 + a["sum_bpaper"] / 380.0
        w2 = a["sum_apcs"] / 350.0 + a["sum_apaper"] / 380.0
        total = w1 + w2
        if abs(total) < 1e-12:
            continue
        store_rows.append(
            {
                "delivery_date": a["delivery_date"],
                "store_id": a["store_id"],
                "store_name": a["store_name"],
                "sum_rpcs": round(a["sum_rpcs"], 6),
                "sum_rcase": round(a["sum_rcase"], 6),
                "sum_rpaper": round(a["sum_rpaper"], 6),
                "sum_bpcs": round(a["sum_bpcs"], 6),
                "sum_bpaper": round(a["sum_bpaper"], 6),
                "sum_apcs": round(a["sum_apcs"], 6),
                "sum_apaper": round(a["sum_apaper"], 6),
                "load_w1": round(w1, 6),
                "load_w2": round(w2, 6),
                "total_load": round(total, 6),
                "original_route_ids": json.dumps(sorted(list(a["routes"])), ensure_ascii=False),
                "original_vehicle_ids": json.dumps(sorted(list(a["vehicles"])), ensure_ascii=False),
                "calc_version": CALC_VERSION,
            }
        )

    vehicle_rows = []
    for (_d, _vid), v in sorted(veh_agg.items(), key=lambda x: (x[0][0], x[0][1])):
        vehicle_rows.append(
            {
                "delivery_date": v["delivery_date"],
                "vehicle_id": v["vehicle_id"],
                "driver_name": v["driver_name"],
                "original_route_ids": json.dumps(sorted(list(v["routes"])), ensure_ascii=False),
                "route_count": len(v["routes"]),
                "calc_version": CALC_VERSION,
            }
        )

    with mysql_conn() as conn:
        with conn.cursor() as c:
            ensure_tables(c)
            c.execute("DELETE FROM chaos_replay_tasks WHERE calc_version=%s", (CALC_VERSION,))
            c.execute("DELETE FROM chaos_replay_task_stores WHERE calc_version=%s", (CALC_VERSION,))
            c.execute("DELETE FROM chaos_replay_task_vehicles WHERE calc_version=%s", (CALC_VERSION,))

            task_sql = """
                INSERT INTO chaos_replay_tasks (
                  delivery_date, day_type, store_count, vehicle_count, route_count,
                  total_load_w1, total_load_w2, total_load, source, calc_version
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            for r in task_rows:
                c.execute(
                    task_sql,
                    (
                        r["delivery_date"],
                        r["day_type"],
                        r["store_count"],
                        r["vehicle_count"],
                        r["route_count"],
                        r["total_load_w1"],
                        r["total_load_w2"],
                        r["total_load"],
                        r["source"],
                        r["calc_version"],
                    ),
                )

            store_sql = """
                INSERT INTO chaos_replay_task_stores (
                  delivery_date, store_id, store_name,
                  sum_rpcs, sum_rcase, sum_rpaper, sum_bpcs, sum_bpaper, sum_apcs, sum_apaper,
                  load_w1, load_w2, total_load, original_route_ids, original_vehicle_ids, calc_version
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            for r in store_rows:
                c.execute(
                    store_sql,
                    (
                        r["delivery_date"],
                        r["store_id"],
                        r["store_name"],
                        r["sum_rpcs"],
                        r["sum_rcase"],
                        r["sum_rpaper"],
                        r["sum_bpcs"],
                        r["sum_bpaper"],
                        r["sum_apcs"],
                        r["sum_apaper"],
                        r["load_w1"],
                        r["load_w2"],
                        r["total_load"],
                        r["original_route_ids"],
                        r["original_vehicle_ids"],
                        r["calc_version"],
                    ),
                )

            veh_sql = """
                INSERT INTO chaos_replay_task_vehicles (
                  delivery_date, vehicle_id, driver_name, original_route_ids, route_count, calc_version
                ) VALUES (%s,%s,%s,%s,%s,%s)
            """
            for r in vehicle_rows:
                c.execute(
                    veh_sql,
                    (
                        r["delivery_date"],
                        r["vehicle_id"],
                        r["driver_name"],
                        r["original_route_ids"],
                        r["route_count"],
                        r["calc_version"],
                    ),
                )

    dates = sorted(date_set)
    date_start = dates[0] if dates else ""
    date_end = dates[-1] if dates else ""

    no_vehicle_days = sum(1 for r in task_rows if int(r["vehicle_count"]) == 0)
    no_store_days = sum(1 for r in task_rows if int(r["store_count"]) == 0)

    print(f"processed_dates={len(date_set)}")
    print(f"written_task_days={len(task_rows)}")
    print(f"written_store_rows={len(store_rows)}")
    print(f"written_vehicle_rows={len(vehicle_rows)}")
    print(f"date_start={date_start}")
    print(f"date_end={date_end}")
    print(f"has_no_vehicle_dates={'YES' if no_vehicle_days > 0 else 'NO'}")
    print(f"has_no_store_dates={'YES' if no_store_days > 0 else 'NO'}")


if __name__ == "__main__":
    main()
