"""
为“平日双配W1缺失段”相关门店补齐坐标（调用高德地理编码）。
仅回填 C_SHOP_MAIN 坐标字段，不改其他业务逻辑。
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


def detect_geo_columns() -> Tuple[str, str]:
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute("SHOW COLUMNS FROM C_SHOP_MAIN")
            cols = {str(r["Field"]) for r in (c.fetchall() or [])}
    if "lng" in cols and "lat" in cols:
        return "lng", "lat"
    if "longitude" in cols and "latitude" in cols:
        return "longitude", "latitude"
    raise RuntimeError("C_SHOP_MAIN缺少坐标字段")


def load_missing_stores() -> List[str]:
    missing_pairs, _ = seg.build_missing_pairs()
    stores = sorted({s for a, b in missing_pairs for s in (a, b) if s != "DC"})
    return stores


def load_store_names(store_ids: List[str]) -> Dict[str, str]:
    name_map = {}
    if not store_ids:
        return name_map
    with mysql_conn() as conn:
        with conn.cursor() as c:
            ph = ",".join(["%s"] * len(store_ids))
            c.execute(
                f"""
                SELECT store_id, store_name
                FROM human_dispatch_routes
                WHERE store_id IN ({ph})
                """,
                store_ids,
            )
            for r in c.fetchall() or []:
                sid = str((r or {}).get("store_id") or "").strip()
                sname = str((r or {}).get("store_name") or "").strip()
                if sid and sname and sid not in name_map:
                    name_map[sid] = sname
    return name_map


def geocode(name: str):
    params = {
        "key": AMAP_KEY,
        "address": name,
        "city": "北京",
    }
    try:
        resp = requests.get(GEOCODE_URL, params=params, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if str(data.get("status")) == "1" and int(data.get("count") or 0) > 0:
            geocodes = data.get("geocodes") or []
            if geocodes:
                loc = str((geocodes[0] or {}).get("location") or "").strip()
                if "," in loc:
                    lng_s, lat_s = loc.split(",", 1)
                    return float(lng_s), float(lat_s)
    except Exception:
        return None
    return None


def write_coords(rows: List[Tuple[float, float, str]], lng_col: str, lat_col: str):
    if not rows:
        return
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.executemany(
                f"""
                UPDATE C_SHOP_MAIN
                SET `{lng_col}`=%s, `{lat_col}`=%s
                WHERE shop_code=%s
                """,
                rows,
            )


def main():
    if not AMAP_KEY:
        raise RuntimeError("AMAP key missing")

    lng_col, lat_col = detect_geo_columns()
    stores = load_missing_stores()
    name_map = load_store_names(stores)

    # 先看哪些店当前坐标为空
    still_missing = []
    with mysql_conn() as conn:
        with conn.cursor() as c:
            ph = ",".join(["%s"] * len(stores)) if stores else "'__none__'"
            c.execute(
                f"""
                SELECT shop_code, `{lng_col}` AS lng, `{lat_col}` AS lat
                FROM C_SHOP_MAIN
                WHERE shop_code IN ({ph})
                """,
                stores if stores else None,
            )
            for r in c.fetchall() or []:
                sid = str((r or {}).get("shop_code") or "").strip()
                lng = (r or {}).get("lng")
                lat = (r or {}).get("lat")
                if sid and (lng is None or lat is None):
                    still_missing.append(sid)

    updated = []
    unresolved = []
    for sid in still_missing:
        sname = name_map.get(sid, "")
        if not sname:
            unresolved.append((sid, "name_missing"))
            continue
        pt = geocode(sname)
        if not pt:
            unresolved.append((sid, "geocode_failed"))
            continue
        updated.append((pt[0], pt[1], sid))
        time.sleep(0.15)

    write_coords(updated, lng_col, lat_col)

    print(f"stores_in_missing_pairs={len(stores)}")
    print(f"stores_coord_null_before={len(still_missing)}")
    print(f"coords_updated={len(updated)}")
    print(f"unresolved={len(unresolved)}")
    if unresolved:
        print("unresolved_sample=", unresolved[:30])


if __name__ == "__main__":
    main()

