from __future__ import annotations

import csv
import sys
from collections import Counter
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

CALC_VERSION = "route_distance_load_profiles_v1"
TOOLS_DIR = PROJECT_ROOT / "tools"

INPUT_FILES = [
    TOOLS_DIR / "other_route_load_distance.csv",
    TOOLS_DIR / "w2_route_load_distance.csv",
    TOOLS_DIR / "w1_route_load_distance_le120.csv",
    TOOLS_DIR / "w1_route_load_distance_121_128.csv",
    TOOLS_DIR / "w1_route_load_distance_129_133.csv",
    TOOLS_DIR / "w1_route_load_distance_eq140.csv",
    TOOLS_DIR / "w1_route_load_distance_gt140.csv",
]


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


def ensure_table(c):
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS rengong_route_distance_load_profiles (
          id BIGINT NOT NULL AUTO_INCREMENT,
          delivery_date DATE NOT NULL,
          vehicle_id VARCHAR(128) NULL,
          route_id INT NOT NULL,
          route_name VARCHAR(255) NULL,
          route_group VARCHAR(16) NOT NULL,
          route_type VARCHAR(64) NOT NULL,
          route_type_detail VARCHAR(128) NULL,
          store_count INT NOT NULL DEFAULT 0,
          one_way_distance_km DECIMAL(18,3) NOT NULL DEFAULT 0,
          route_load DECIMAL(18,6) NOT NULL DEFAULT 0,
          load_formula_scope VARCHAR(16) NOT NULL,
          is_city TINYINT(1) NOT NULL DEFAULT 0,
          is_airport TINYINT(1) NOT NULL DEFAULT 0,
          is_suburban TINYINT(1) NOT NULL DEFAULT 0,
          is_outer_city TINYINT(1) NOT NULL DEFAULT 0,
          is_metro TINYINT(1) NOT NULL DEFAULT 0,
          is_highspeed_station TINYINT(1) NOT NULL DEFAULT 0,
          normal_load_limit DECIMAL(10,4) NULL,
          is_over_normal_limit TINYINT(1) NOT NULL DEFAULT 0,
          is_dirty_candidate TINYINT(1) NOT NULL DEFAULT 0,
          source_file VARCHAR(255) NOT NULL,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_calc_version (calc_version),
          KEY idx_delivery_route (delivery_date, route_id),
          KEY idx_route_group_type (route_group, route_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def classify_w1(route_id: int, route_name: str):
    if route_id <= 120:
        return "市区", None
    if 121 <= route_id <= 128:
        return "郊区周边", None
    if 129 <= route_id <= 133:
        return "地铁", None
    if route_id == 140:
        return "机场", "首都机场" if "首都机场" in route_name else None
    if route_id > 140:
        detail = route_name.strip() or None
        return "周边城市", detail
    return "市区", None


def classify_w2(route_name: str):
    n = (route_name or "").strip()
    if "机场" in n:
        return "机场", "首都机场" if "首都机场" in n else n or None
    return "市区", None


def load_rows():
    out = []
    for fp in INPUT_FILES:
        if not fp.exists():
            raise FileNotFoundError(f"输入文件不存在: {fp}")

        with fp.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            headers = set(reader.fieldnames or [])
            for r in reader:
                delivery_date = str((r or {}).get("delivery_date") or "").strip()
                route_id_raw = str((r or {}).get("route_id") or "").strip()
                if not delivery_date or not route_id_raw:
                    continue

                route_id = _to_int(route_id_raw)
                route_name = str((r or {}).get("route_name") or "").strip()
                vehicle_id = str((r or {}).get("vehicle_id_list") or "").strip()
                store_count = _to_int((r or {}).get("store_count"))
                one_way_distance_km = _to_float((r or {}).get("trip_distance_km_one_way"))

                if "route_load_w1" in headers:
                    route_group = "W1"
                    load_formula_scope = "W1"
                    route_load = _to_float((r or {}).get("route_load_w1"))
                    route_type, route_type_detail = classify_w1(route_id, route_name)
                elif "route_load_w2" in headers:
                    route_group = "W2"
                    load_formula_scope = "W2"
                    route_load = _to_float((r or {}).get("route_load_w2"))
                    route_type, route_type_detail = classify_w2(route_name)
                elif "route_load_other" in headers:
                    route_group = "OTHER"
                    load_formula_scope = "OTHER"
                    route_load = _to_float((r or {}).get("route_load_other"))
                    route_type, route_type_detail = ("高铁站", route_name.strip() or None)
                else:
                    raise RuntimeError(f"无法识别装载列: {fp.name}")

                is_city = 1 if route_type == "市区" else 0
                is_airport = 1 if route_type == "机场" else 0
                is_suburban = 1 if route_type == "郊区周边" else 0
                is_outer_city = 1 if route_type == "周边城市" else 0
                is_metro = 1 if route_type == "地铁" else 0
                is_highspeed_station = 1 if route_type == "高铁站" else 0

                normal_load_limit = 1.25 if (route_group == "W1" and route_type == "市区") else None
                is_over_normal_limit = 1 if (normal_load_limit is not None and route_load > normal_load_limit) else 0
                is_dirty_candidate = 1 if (not is_airport and route_load > 5.0) else 0

                out.append(
                    {
                        "delivery_date": delivery_date,
                        "vehicle_id": vehicle_id,
                        "route_id": route_id,
                        "route_name": route_name,
                        "route_group": route_group,
                        "route_type": route_type,
                        "route_type_detail": route_type_detail,
                        "store_count": store_count,
                        "one_way_distance_km": round(one_way_distance_km, 3),
                        "route_load": round(route_load, 6),
                        "load_formula_scope": load_formula_scope,
                        "is_city": is_city,
                        "is_airport": is_airport,
                        "is_suburban": is_suburban,
                        "is_outer_city": is_outer_city,
                        "is_metro": is_metro,
                        "is_highspeed_station": is_highspeed_station,
                        "normal_load_limit": normal_load_limit,
                        "is_over_normal_limit": is_over_normal_limit,
                        "is_dirty_candidate": is_dirty_candidate,
                        "source_file": fp.name,
                        "calc_version": CALC_VERSION,
                    }
                )
    return out


def persist(rows):
    with mysql_conn() as conn:
        with conn.cursor() as c:
            ensure_table(c)
            c.execute(
                "DELETE FROM rengong_route_distance_load_profiles WHERE calc_version=%s",
                (CALC_VERSION,),
            )
            sql = """
                INSERT INTO rengong_route_distance_load_profiles (
                  delivery_date, vehicle_id, route_id, route_name,
                  route_group, route_type, route_type_detail,
                  store_count, one_way_distance_km, route_load, load_formula_scope,
                  is_city, is_airport, is_suburban, is_outer_city, is_metro, is_highspeed_station,
                  normal_load_limit, is_over_normal_limit, is_dirty_candidate,
                  source_file, calc_version
                ) VALUES (
                  %s,%s,%s,%s,
                  %s,%s,%s,
                  %s,%s,%s,%s,
                  %s,%s,%s,%s,%s,%s,
                  %s,%s,%s,
                  %s,%s
                )
            """
            for r in rows:
                c.execute(
                    sql,
                    (
                        r["delivery_date"],
                        r["vehicle_id"],
                        r["route_id"],
                        r["route_name"],
                        r["route_group"],
                        r["route_type"],
                        r["route_type_detail"],
                        r["store_count"],
                        r["one_way_distance_km"],
                        r["route_load"],
                        r["load_formula_scope"],
                        r["is_city"],
                        r["is_airport"],
                        r["is_suburban"],
                        r["is_outer_city"],
                        r["is_metro"],
                        r["is_highspeed_station"],
                        r["normal_load_limit"],
                        r["is_over_normal_limit"],
                        r["is_dirty_candidate"],
                        r["source_file"],
                        r["calc_version"],
                    ),
                )


def print_stats(rows):
    type_counter = Counter([r["route_type"] for r in rows])
    group_counter = Counter([r["route_group"] for r in rows])
    max_dist = max([_to_float(r["one_way_distance_km"]) for r in rows], default=0.0)
    max_load = max([_to_float(r["route_load"]) for r in rows], default=0.0)
    dirty_cnt = sum([1 for r in rows if int(r["is_dirty_candidate"]) == 1])

    print(f"written_total_rows={len(rows)}")
    print("rows_by_route_type=" + ", ".join([f"{k}:{v}" for k, v in sorted(type_counter.items())]))
    print("rows_by_route_group=" + ", ".join([f"{k}:{v}" for k, v in sorted(group_counter.items())]))
    print(f"max_one_way_distance_km={round(max_dist, 3)}")
    print(f"max_route_load={round(max_load, 6)}")
    print(f"dirty_candidate_count={dirty_cnt}")
    print(f"calc_version={CALC_VERSION}")


def main():
    rows = load_rows()
    persist(rows)
    print_stats(rows)


if __name__ == "__main__":
    main()
