from __future__ import annotations

import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

CALC_VERSION = "route_load_w1_v1"
NORMAL_HARD_LIMIT = 1.25


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


def ensure_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rengong_route_load_profiles (
          id BIGINT NOT NULL AUTO_INCREMENT,
          delivery_date DATE NOT NULL,
          vehicle_id VARCHAR(64) NULL,
          route_id INT NOT NULL,
          route_name VARCHAR(255) NULL,
          route_category VARCHAR(128) NULL,
          store_count INT NOT NULL DEFAULT 0,
          sum_rpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_rcase DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_rpaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_bpcs DECIMAL(18,6) NOT NULL DEFAULT 0,
          sum_bpaper DECIMAL(18,6) NOT NULL DEFAULT 0,
          route_load_w1 DECIMAL(18,6) NOT NULL DEFAULT 0,
          is_airport_route BOOLEAN NOT NULL DEFAULT FALSE,
          normal_hard_limit DECIMAL(10,4) NOT NULL DEFAULT 1.25,
          is_over_normal_limit BOOLEAN NOT NULL DEFAULT FALSE,
          is_dirty_candidate BOOLEAN NOT NULL DEFAULT FALSE,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_calc_version (calc_version),
          KEY idx_date_route (delivery_date, route_id)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def is_airport_route_name(route_name: str):
    text = str(route_name or "").strip()
    if not text:
        return False
    keys = ("首都机场", "机场")
    return any(k in text for k in keys)


def main():
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT delivery_date, vehicle_id, route_id, route_name, stop_sequence, store_id
                FROM human_dispatch_routes
                ORDER BY delivery_date, route_id, stop_sequence
                """
            )
            route_rows = c.fetchall() or []

            c.execute(
                """
                SELECT delivery_date, shop_code, rpcs, rcase, rpaper, bpcs, bpaper
                FROM human_dispatch_solver_ready
                """
            )
            solver_rows = c.fetchall() or []

    store_load_map = {}
    raw_map = {}
    for r in solver_rows:
        d = str((r or {}).get("delivery_date") or "").strip()
        sid = str((r or {}).get("shop_code") or "").strip()
        if not d or not sid:
            continue
        rpcs = _to_float((r or {}).get("rpcs"))
        rcase = _to_float((r or {}).get("rcase"))
        rpaper = _to_float((r or {}).get("rpaper"))
        bpcs = _to_float((r or {}).get("bpcs"))
        bpaper = _to_float((r or {}).get("bpaper"))
        raw_map[(d, sid)] = {
            "rpcs": rpcs,
            "rcase": rcase,
            "rpaper": rpaper,
            "bpcs": bpcs,
            "bpaper": bpaper,
        }
        store_load_map[(d, sid)] = (
            rpcs / 207.0 + rcase / 380.0 + rpaper / 380.0 + bpcs / 120.0 + bpaper / 380.0
        )

    # key: (delivery_date, route_id)
    route_stops = defaultdict(dict)  # stop_key(seq, sid) -> meta
    for r in route_rows:
        d = str((r or {}).get("delivery_date") or "").strip()
        rid_raw = str((r or {}).get("route_id") or "").strip()
        sid = str((r or {}).get("store_id") or "").strip()
        if not d or not rid_raw or not sid:
            continue
        # route_id 以 1 开头才是 W1
        if not rid_raw.startswith("1"):
            continue
        rid = int(_to_float(rid_raw))
        seq = int(_to_float((r or {}).get("stop_sequence")))
        key = (d, rid)
        stop_key = (seq, sid)
        if stop_key not in route_stops[key]:
            route_stops[key][stop_key] = {
                "vehicle_id": str((r or {}).get("vehicle_id") or "").strip(),
                "route_name": str((r or {}).get("route_name") or "").strip(),
                "store_id": sid,
            }

    output_rows = []
    for (d, rid), stops_map in sorted(route_stops.items(), key=lambda x: (x[0][0], x[0][1])):
        stops = sorted(stops_map.items(), key=lambda x: x[0][0])
        vehicle_ids = sorted(
            {str((meta or {}).get("vehicle_id") or "").strip() for _, meta in stops if str((meta or {}).get("vehicle_id") or "").strip()}
        )
        route_name = ""
        for _, meta in stops:
            rn = str((meta or {}).get("route_name") or "").strip()
            if rn:
                route_name = rn
                break
        is_airport = is_airport_route_name(route_name)
        route_category = "首都机场" if is_airport else "normal"

        sum_rpcs = 0.0
        sum_rcase = 0.0
        sum_rpaper = 0.0
        sum_bpcs = 0.0
        sum_bpaper = 0.0
        route_load = 0.0

        for (_, sid), _meta in stops:
            raw = raw_map.get((d, sid), {})
            sum_rpcs += _to_float(raw.get("rpcs"))
            sum_rcase += _to_float(raw.get("rcase"))
            sum_rpaper += _to_float(raw.get("rpaper"))
            sum_bpcs += _to_float(raw.get("bpcs"))
            sum_bpaper += _to_float(raw.get("bpaper"))
            route_load += _to_float(store_load_map.get((d, sid)))

        route_load = round(route_load, 6)
        over_limit = (route_load > NORMAL_HARD_LIMIT) and (not is_airport)
        dirty = (route_load > 5) and (not is_airport)

        output_rows.append(
            {
                "delivery_date": d,
                "vehicle_id": ",".join(vehicle_ids),
                "route_id": rid,
                "route_name": route_name,
                "route_category": route_category,
                "store_count": len(stops),
                "sum_rpcs": round(sum_rpcs, 6),
                "sum_rcase": round(sum_rcase, 6),
                "sum_rpaper": round(sum_rpaper, 6),
                "sum_bpcs": round(sum_bpcs, 6),
                "sum_bpaper": round(sum_bpaper, 6),
                "route_load_w1": route_load,
                "is_airport_route": is_airport,
                "normal_hard_limit": NORMAL_HARD_LIMIT,
                "is_over_normal_limit": over_limit,
                "is_dirty_candidate": dirty,
                "calc_version": CALC_VERSION,
            }
        )

    with mysql_conn() as conn:
        with conn.cursor() as c:
            ensure_table(c)
            c.execute("DELETE FROM rengong_route_load_profiles WHERE calc_version=%s", (CALC_VERSION,))
            ins = """
                INSERT INTO rengong_route_load_profiles (
                  delivery_date, vehicle_id, route_id, route_name, route_category, store_count,
                  sum_rpcs, sum_rcase, sum_rpaper, sum_bpcs, sum_bpaper, route_load_w1,
                  is_airport_route, normal_hard_limit, is_over_normal_limit, is_dirty_candidate,
                  calc_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """
            for r in output_rows:
                c.execute(
                    ins,
                    (
                        r["delivery_date"],
                        r["vehicle_id"],
                        r["route_id"],
                        r["route_name"],
                        r["route_category"],
                        r["store_count"],
                        r["sum_rpcs"],
                        r["sum_rcase"],
                        r["sum_rpaper"],
                        r["sum_bpcs"],
                        r["sum_bpaper"],
                        r["route_load_w1"],
                        r["is_airport_route"],
                        r["normal_hard_limit"],
                        r["is_over_normal_limit"],
                        r["is_dirty_candidate"],
                        r["calc_version"],
                    ),
                )

    written = len(output_rows)
    dates = [str(r["delivery_date"]) for r in output_rows] if output_rows else []
    date_start = min(dates) if dates else ""
    date_end = max(dates) if dates else ""
    max_load = max([_to_float(r["route_load_w1"]) for r in output_rows], default=0.0)
    over_cnt = sum(1 for r in output_rows if bool(r["is_over_normal_limit"]))
    dirty_cnt = sum(1 for r in output_rows if bool(r["is_dirty_candidate"]))

    print(f"written_routes={written}")
    print(f"date_start={date_start}")
    print(f"date_end={date_end}")
    print(f"max_route_load_w1={round(max_load, 6)}")
    print(f"over_normal_limit_count={over_cnt}")
    print(f"dirty_candidate_count={dirty_cnt}")
    print(f"calc_version={CALC_VERSION}")


if __name__ == "__main__":
    main()
