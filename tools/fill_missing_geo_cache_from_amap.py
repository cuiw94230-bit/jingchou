"""
经验模块专用：把缺失段涉及门店的坐标写入 rengong_store_geo_cache。
严格不写 C_SHOP_MAIN。
"""

from __future__ import annotations

import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Set, Tuple

import pymysql
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "tools"))

from config import settings
import fill_missing_amap_segments_weekday_dual_w1 as seg

AMAP_KEY = (
    settings.AMAP_WEB_SERVICE_KEY
    or settings.AMAP_JS_WEB_KEY
    or os.getenv("AMAP_WEB_SERVICE_KEY", "").strip()
    or "3ba73c0e0906dcbe77eeb85f3a5c343d"
)
GEOCODE_URL = "https://restapi.amap.com/v3/geocode/geo"
RENGONG_GEO_TABLE = "rengong_store_geo_cache"


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


def ensure_geo_cache_table():
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {RENGONG_GEO_TABLE} (
                  store_id VARCHAR(32) NOT NULL PRIMARY KEY,
                  store_name VARCHAR(255) NULL,
                  lng DECIMAL(12,7) NOT NULL,
                  lat DECIMAL(12,7) NOT NULL,
                  source VARCHAR(32) NOT NULL DEFAULT 'amap_geocode',
                  updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
                )
                """
            )


def load_missing_stores() -> List[str]:
    missing_pairs, _ = seg.build_missing_pairs()
    stores = sorted({s for a, b in missing_pairs for s in (a, b) if s != "DC"})
    return stores


def load_store_names(stores: List[str]) -> Dict[str, str]:
    if not stores:
        return {}
    names = {}
    with mysql_conn() as conn:
        with conn.cursor() as c:
            ph = ",".join(["%s"] * len(stores))
            c.execute(
                f"""
                SELECT store_id, MAX(store_name) AS store_name
                FROM human_dispatch_routes
                WHERE store_id IN ({ph})
                GROUP BY store_id
                """,
                stores,
            )
            for row in c.fetchall() or []:
                sid = str((row or {}).get("store_id") or "").strip()
                sname = str((row or {}).get("store_name") or "").strip()
                if sid and sname:
                    names[sid] = sname
    return names


def load_already_cached(stores: List[str]) -> Set[str]:
    if not stores:
        return set()
    with mysql_conn() as conn:
        with conn.cursor() as c:
            ph = ",".join(["%s"] * len(stores))
            c.execute(
                f"SELECT store_id FROM {RENGONG_GEO_TABLE} WHERE store_id IN ({ph})",
                stores,
            )
            return {str((r or {}).get("store_id") or "").strip() for r in (c.fetchall() or [])}


def _parse_geocode(data):
    if str(data.get("status")) == "1" and int(data.get("count") or 0) > 0:
        geocodes = data.get("geocodes") or []
        if geocodes:
            loc = str((geocodes[0] or {}).get("location") or "").strip()
            if "," in loc:
                lng, lat = loc.split(",", 1)
                return float(lng), float(lat)
    return None


def geocode_beijing(address: str):
    candidates = [
        address,
        address.replace("店", ""),
        address.replace("超市发", ""),
        f"北京{address}",
        f"北京{address.replace('店', '')}",
    ]
    seen = set()
    for text in candidates:
        q = str(text or "").strip()
        if not q or q in seen:
            continue
        seen.add(q)
        params = {"key": AMAP_KEY, "address": q, "city": "北京"}
        try:
            resp = requests.get(GEOCODE_URL, params=params, timeout=15)
            resp.raise_for_status()
            point = _parse_geocode(resp.json())
            if point:
                return point
        except Exception:
            pass
        time.sleep(0.08)
    return None


def upsert_cache(rows: List[Tuple[str, str, float, float]]):
    if not rows:
        return
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.executemany(
                f"""
                INSERT INTO {RENGONG_GEO_TABLE}
                (store_id, store_name, lng, lat, source, updated_at)
                VALUES (%s,%s,%s,%s,'amap_geocode',NOW())
                ON DUPLICATE KEY UPDATE
                  store_name=VALUES(store_name),
                  lng=VALUES(lng),
                  lat=VALUES(lat),
                  source='amap_geocode',
                  updated_at=NOW()
                """,
                rows,
            )


def main():
    if not AMAP_KEY:
        raise RuntimeError("AMAP key missing")
    ensure_geo_cache_table()
    stores = load_missing_stores()
    names = load_store_names(stores)
    cached = load_already_cached(stores)
    todo = [s for s in stores if s not in cached]

    ok_rows = []
    failed = []
    for sid in todo:
        sname = names.get(sid, "")
        if not sname:
            failed.append((sid, "name_missing"))
            continue
        point = geocode_beijing(sname)
        if not point:
            failed.append((sid, "geocode_failed"))
            continue
        ok_rows.append((sid, sname, point[0], point[1]))
        time.sleep(0.12)

    upsert_cache(ok_rows)
    print(f"missing_store_total={len(stores)}")
    print(f"already_cached={len(cached)}")
    print(f"new_cached={len(ok_rows)}")
    print(f"failed={len(failed)}")
    if failed:
        print("failed_sample=", failed[:30])


if __name__ == "__main__":
    main()
