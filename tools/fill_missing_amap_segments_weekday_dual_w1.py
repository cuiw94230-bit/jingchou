"""
补齐“平日市内双配波次一”线路中的缺失段（高德真实驾车距离+时长），并落库到 store_distance_matrix。

严格规则：
- 只补缺失段
- 只用高德驾车 API（v3/distance, type=1）
- 不用直线距离，不做推测
"""

from __future__ import annotations

import json
import os
import time
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pymysql
import requests

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

DC_ID = "DC"
DC_LNG = 116.568327
DC_LAT = 40.082845

AMAP_URL = "https://restapi.amap.com/v3/distance"
AMAP_KEY = (
    settings.AMAP_WEB_SERVICE_KEY
    or settings.AMAP_JS_WEB_KEY
    or os.getenv("AMAP_WEB_SERVICE_KEY", "").strip()
    or "3ba73c0e0906dcbe77eeb85f3a5c343d"
)
STORE_DISTANCE_TABLE = "store_distance_matrix"
RENGONG_GEO_TABLE = "rengong_store_geo_cache"
SOURCE_TAG = "amap_driving_real_fill"

CHUNK_SIZE = 20
QPS_SLEEP = 0.25
TIMEOUT = 20
MAX_RETRY = 4


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


def is_sunday(date_text: str) -> bool:
    return datetime.strptime(date_text, "%Y-%m-%d").weekday() == 6


def detect_geo_columns() -> Tuple[str, str]:
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute("SHOW COLUMNS FROM C_SHOP_MAIN")
            cols = {str(r["Field"]) for r in (c.fetchall() or [])}
    if "lng" in cols and "lat" in cols:
        return "lng", "lat"
    if "longitude" in cols and "latitude" in cols:
        return "longitude", "latitude"
    raise RuntimeError("C_SHOP_MAIN缺少坐标字段(lng/lat 或 longitude/latitude)")


def load_store_points(store_ids: Set[str]) -> Dict[str, Tuple[float, float]]:
    points = {DC_ID: (DC_LNG, DC_LAT)}
    if not store_ids:
        return points
    lng_col, lat_col = detect_geo_columns()
    ids = sorted([x for x in store_ids if x and x != DC_ID])
    with mysql_conn() as conn:
        with conn.cursor() as c:
            placeholders = ",".join(["%s"] * len(ids))
            c.execute(
                f"""
                SELECT shop_code, `{lng_col}` AS lng, `{lat_col}` AS lat
                FROM C_SHOP_MAIN
                WHERE shop_code IN ({placeholders})
                  AND `{lng_col}` IS NOT NULL
                  AND `{lat_col}` IS NOT NULL
                """,
                ids,
            )
            for row in c.fetchall() or []:
                sid = str((row or {}).get("shop_code") or "").strip()
                if not sid:
                    continue
                try:
                    points[sid] = (float(row["lng"]), float(row["lat"]))
                except Exception:
                    continue
            # 经验专用坐标缓存（不影响调度主表）
            try:
                placeholders = ",".join(["%s"] * len(ids))
                c.execute(
                    f"""
                    SELECT store_id, lng, lat
                    FROM {RENGONG_GEO_TABLE}
                    WHERE store_id IN ({placeholders})
                    """,
                    ids,
                )
                for row in c.fetchall() or []:
                    sid = str((row or {}).get("store_id") or "").strip()
                    if not sid:
                        continue
                    try:
                        lng = float(row["lng"])
                        lat = float(row["lat"])
                    except Exception:
                        continue
                    points[sid] = (lng, lat)
            except Exception:
                # 表不存在时保持静默，避免影响主流程
                pass
    return points


def load_weekday_dual_w1_routes() -> Dict[Tuple[str, str, int], List[str]]:
    """
    返回: {(delivery_date, vehicle_id, route_id): [按stop_sequence排序的store_id...]}
    仅平日 + 双配第一趟(route_id=X 且存在 X+100)
    """
    rows = []
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT delivery_date, vehicle_id, route_id, stop_sequence, store_id
                FROM human_dispatch_routes
                ORDER BY delivery_date, vehicle_id, route_id, stop_sequence
                """
            )
            rows = c.fetchall() or []

    routes_by_day_vehicle = defaultdict(set)
    raw_route_stops = defaultdict(list)
    for r in rows:
        d = str((r or {}).get("delivery_date") or "").strip()
        v = str((r or {}).get("vehicle_id") or "").strip()
        sid = str((r or {}).get("store_id") or "").strip()
        try:
            rid = int(float((r or {}).get("route_id") or 0))
            seq = int(float((r or {}).get("stop_sequence") or 0))
        except Exception:
            continue
        if not d or not v or rid <= 0 or not sid:
            continue
        if is_sunday(d):
            continue
        routes_by_day_vehicle[(d, v)].add(rid)
        raw_route_stops[(d, v, rid)].append((seq, sid))

    picked = {}
    for (d, v), route_ids in routes_by_day_vehicle.items():
        first_ids = sorted([x for x in route_ids if (x + 100) in route_ids])
        for rid in first_ids:
            seq_stores = sorted(raw_route_stops.get((d, v, rid), []), key=lambda x: x[0])
            stores = [s for _, s in seq_stores if s]
            if stores:
                picked[(d, v, rid)] = stores
    return picked


def load_existing_pairs(candidate_pairs: Set[Tuple[str, str]]) -> Set[Tuple[str, str]]:
    if not candidate_pairs:
        return set()
    pair_list = sorted(candidate_pairs)
    existing = set()
    with mysql_conn() as conn:
        with conn.cursor() as c:
            chunk = 1000
            for i in range(0, len(pair_list), chunk):
                sub = pair_list[i : i + chunk]
                values_sql = ",".join(["(%s,%s)"] * len(sub))
                params = []
                for a, b in sub:
                    params.extend([a, b])
                c.execute(
                    f"""
                    SELECT m.from_store_id, m.to_store_id
                    FROM {STORE_DISTANCE_TABLE} m
                    JOIN (VALUES {values_sql}) AS t(from_id, to_id)
                      ON m.from_store_id=t.from_id AND m.to_store_id=t.to_id
                    """,
                    params,
                )
                for r in c.fetchall() or []:
                    existing.add((str(r["from_store_id"]), str(r["to_store_id"])))
    return existing


def load_existing_pairs_fallback(candidate_pairs: Set[Tuple[str, str]]) -> Set[Tuple[str, str]]:
    """
    MySQL 不支持 VALUES table alias 的版本兜底实现。
    """
    if not candidate_pairs:
        return set()
    from_ids = sorted({a for a, _ in candidate_pairs})
    to_ids = sorted({b for _, b in candidate_pairs})
    existing = set()
    with mysql_conn() as conn:
        with conn.cursor() as c:
            ph1 = ",".join(["%s"] * len(from_ids))
            ph2 = ",".join(["%s"] * len(to_ids))
            c.execute(
                f"""
                SELECT from_store_id, to_store_id
                FROM {STORE_DISTANCE_TABLE}
                WHERE from_store_id IN ({ph1}) AND to_store_id IN ({ph2})
                """,
                from_ids + to_ids,
            )
            for r in c.fetchall() or []:
                pair = (str(r["from_store_id"]), str(r["to_store_id"]))
                if pair in candidate_pairs:
                    existing.add(pair)
    return existing


def split_chunks(arr: List[str], n: int):
    for i in range(0, len(arr), n):
        yield arr[i : i + n]


def call_amap(origins: List[Tuple[float, float]], destination: Tuple[float, float]):
    params = {
        "key": AMAP_KEY,
        "origins": "|".join([f"{lng},{lat}" for lng, lat in origins]),
        "destination": f"{destination[0]},{destination[1]}",
        "type": 1,      # 驾车距离
        "strategy": 0,  # 速度优先
    }
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(AMAP_URL, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            if str(data.get("status")) == "1" and isinstance(data.get("results"), list):
                return data.get("results") or []
        except Exception:
            pass
        time.sleep(0.5 * attempt)
    return None


def upsert_distance_rows(rows: List[Tuple[str, str, float, float, str]]):
    if not rows:
        return
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.executemany(
                f"""
                INSERT INTO {STORE_DISTANCE_TABLE}
                (from_store_id, to_store_id, distance_km, duration_minutes, source, updated_at)
                VALUES (%s,%s,%s,%s,%s,NOW())
                ON DUPLICATE KEY UPDATE
                  distance_km=VALUES(distance_km),
                  duration_minutes=VALUES(duration_minutes),
                  source=VALUES(source),
                  updated_at=NOW()
                """,
                rows,
            )


def build_missing_pairs() -> Tuple[Set[Tuple[str, str]], Set[str]]:
    routes = load_weekday_dual_w1_routes()
    all_pairs = set()
    all_store_ids = set()
    for _, stores in routes.items():
        if not stores:
            continue
        all_store_ids.update(stores)
        # DC -> first
        all_pairs.add((DC_ID, stores[0]))
        # store -> store
        for i in range(1, len(stores)):
            all_pairs.add((stores[i - 1], stores[i]))
        # last -> DC
        all_pairs.add((stores[-1], DC_ID))

    # 查询已有
    existing = load_existing_pairs_fallback(all_pairs)
    missing = all_pairs - existing
    return missing, all_store_ids


def main():
    if not AMAP_KEY:
        raise RuntimeError("AMAP_WEB_SERVICE_KEY 未配置，无法调用高德距离API")

    missing_pairs, all_store_ids = build_missing_pairs()
    print(f"missing_pairs_before={len(missing_pairs)}")
    if not missing_pairs:
        print("no_missing_pairs")
        return

    points = load_store_points(all_store_ids.union({DC_ID}))
    unresolved_no_coord = 0
    rows_to_upsert = []

    # 按 destination 分桶，批量 origins 调高德
    by_dst = defaultdict(list)
    for src, dst in sorted(missing_pairs):
        by_dst[dst].append(src)

    fetched = 0
    skipped_zero = 0

    for dst, src_list in by_dst.items():
        if dst not in points:
            unresolved_no_coord += len(src_list)
            continue
        dst_pt = points[dst]
        valid_src = [s for s in src_list if s in points]
        unresolved_no_coord += (len(src_list) - len(valid_src))
        for chunk_src in split_chunks(valid_src, CHUNK_SIZE):
            origins = [points[s] for s in chunk_src]
            results = call_amap(origins, dst_pt)
            if results is None or len(results) != len(chunk_src):
                continue
            pack = []
            for i, src in enumerate(chunk_src):
                item = results[i] or {}
                try:
                    dist_m = float(item.get("distance") or 0.0)
                    dur_s = float(item.get("duration") or 0.0)
                except Exception:
                    dist_m = 0.0
                    dur_s = 0.0
                if dist_m <= 0 or dur_s <= 0:
                    skipped_zero += 1
                    continue
                pack.append(
                    (
                        src,
                        dst,
                        round(dist_m / 1000.0, 4),
                        round(dur_s / 60.0, 2),
                        SOURCE_TAG,
                    )
                )
            if pack:
                upsert_distance_rows(pack)
                fetched += len(pack)
            time.sleep(QPS_SLEEP)

    # 再次检查缺失
    missing_after, _ = build_missing_pairs()
    print(f"fetched_upserted={fetched}")
    print(f"skipped_zero={skipped_zero}")
    print(f"unresolved_no_coord={unresolved_no_coord}")
    print(f"missing_pairs_after={len(missing_after)}")


if __name__ == "__main__":
    main()
