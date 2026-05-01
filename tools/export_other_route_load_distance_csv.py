from __future__ import annotations

import csv
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

OUTPUT_CSV = PROJECT_ROOT / "tools" / "other_route_load_distance.csv"
DC_ID = "DC"


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


def _to_int(v):
    return int(round(_to_float(v)))


def _is_sunday(date_text: str):
    from datetime import datetime

    return datetime.strptime(date_text, "%Y-%m-%d").weekday() == 6


def load_route_rows():
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT
                  delivery_date,
                  vehicle_id,
                  route_id,
                  route_name,
                  stop_sequence,
                  store_id,
                  store_name,
                  ambient_turnover_boxes,
                  ambient_cartons,
                  ambient_full_cases,
                  frozen_bags,
                  frozen_cartons
                FROM human_dispatch_routes
                ORDER BY delivery_date, route_id, stop_sequence
                """
            )
            return c.fetchall() or []


def load_distance_matrix():
    d = {}
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute("SELECT * FROM store_distance_matrix LIMIT 1")
            sample = c.fetchone() or {}
            cols = set(sample.keys())

            from_candidates = ["from_store_code", "from_shop_code", "from_code", "origin_store_code", "from_store_id"]
            to_candidates = ["to_store_code", "to_shop_code", "to_code", "dest_store_code", "to_store_id"]
            dist_candidates = ["distance_m", "distance", "distance_meter", "amap_distance_m", "distance_km"]

            from_col = next((x for x in from_candidates if x in cols), None)
            to_col = next((x for x in to_candidates if x in cols), None)
            dist_col = next((x for x in dist_candidates if x in cols), None)

            if not from_col or not to_col or not dist_col:
                c.execute("SHOW COLUMNS FROM store_distance_matrix")
                all_cols = [str((r or {}).get("Field") or "") for r in (c.fetchall() or [])]
                raise RuntimeError(f"store_distance_matrix 字段不匹配，现有字段: {all_cols}")

            c.execute(f"SELECT `{from_col}` AS f, `{to_col}` AS t, `{dist_col}` AS dm FROM store_distance_matrix")
            for r in c.fetchall() or []:
                f = str((r or {}).get("f") or "").strip()
                t = str((r or {}).get("t") or "").strip()
                if not f or not t:
                    continue
                val = _to_float((r or {}).get("dm"))
                if dist_col == "distance_km":
                    val = val * 1000.0
                d[(f, t)] = val
    return d


def main():
    rows = load_route_rows()
    dist_map = load_distance_matrix()

    # 主索引：delivery_date + route_id（不以车号为索引）
    route_stops = defaultdict(dict)  # key -> {(seq, store_id): row}
    for r in rows:
        date_text = str((r or {}).get("delivery_date") or "").strip()
        if not date_text or _is_sunday(date_text):
            continue
        route_id_raw = str((r or {}).get("route_id") or "").strip()
        # 其他线路：排除 1* 和 2*
        if route_id_raw.startswith("1") or route_id_raw.startswith("2"):
            continue
        store_id = str((r or {}).get("store_id") or "").strip()
        if not store_id:
            continue
        seq = _to_int((r or {}).get("stop_sequence"))
        key = (date_text, route_id_raw)
        stop_key = (seq, store_id)
        if stop_key not in route_stops[key]:
            route_stops[key][stop_key] = r

    out = []
    for (delivery_date, route_id), stop_map in sorted(route_stops.items(), key=lambda x: (x[0][0], x[0][1])):
        stops = sorted(stop_map.items(), key=lambda x: x[0][0])

        sum_rpcs = 0.0
        sum_rpaper = 0.0
        sum_rcase = 0.0
        sum_bpcs = 0.0
        sum_bpaper = 0.0

        total_m = 0.0
        prev = DC_ID
        missing_seg_count = 0

        vehicle_ids = set()
        route_name = ""

        for (_seq, sid), row in stops:
            vehicle_ids.add(str((row or {}).get("vehicle_id") or "").strip())
            if not route_name:
                route_name = str((row or {}).get("route_name") or "").strip()

            sum_rpcs += _to_float((row or {}).get("ambient_turnover_boxes"))
            sum_rpaper += _to_float((row or {}).get("ambient_cartons"))
            sum_rcase += _to_float((row or {}).get("ambient_full_cases"))
            sum_bpcs += _to_float((row or {}).get("frozen_bags"))
            sum_bpaper += _to_float((row or {}).get("frozen_cartons"))

            m = dist_map.get((prev, sid))
            if m is None:
                missing_seg_count += 1
            else:
                total_m += m
            prev = sid

        route_load_other = (
            sum_rpcs / 207.0
            + sum_rcase / 380.0
            + sum_rpaper / 380.0
            + sum_bpcs / 120.0
            + sum_bpaper / 380.0
        )

        out.append(
            {
                "delivery_date": delivery_date,
                "route_id": route_id,
                "vehicle_id_list": ",".join(sorted([v for v in vehicle_ids if v])),
                "route_name": route_name,
                "store_count": len(stops),
                "sum_ambient_turnover_boxes": round(sum_rpcs, 3),
                "sum_ambient_cartons": round(sum_rpaper, 3),
                "sum_ambient_full_cases": round(sum_rcase, 3),
                "sum_frozen_bags": round(sum_bpcs, 3),
                "sum_frozen_cartons": round(sum_bpaper, 3),
                "route_load_other": round(route_load_other, 6),
                "trip_distance_km_one_way": round(total_m / 1000.0, 3),
                "missing_segment_count": missing_seg_count,
            }
        )

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "delivery_date",
                "route_id",
                "vehicle_id_list",
                "route_name",
                "store_count",
                "sum_ambient_turnover_boxes",
                "sum_ambient_cartons",
                "sum_ambient_full_cases",
                "sum_frozen_bags",
                "sum_frozen_cartons",
                "route_load_other",
                "trip_distance_km_one_way",
                "missing_segment_count",
            ],
        )
        writer.writeheader()
        writer.writerows(out)

    print(f"[OK] rows={len(out)} output={OUTPUT_CSV}")


if __name__ == "__main__":
    main()
