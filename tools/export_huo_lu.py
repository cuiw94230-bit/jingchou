"""货量/线路导出脚本：把后端接口结果导出为 xlsx/txt 供人工核对。"""`n`nimport json
import math
import os
import urllib.parse
import urllib.request
from datetime import datetime

from openpyxl import Workbook


PROJECT_ROOT = r"C:\Users\laoj0\Desktop\一二技术\城市便利店调度系统"
BASE_URL = "http://127.0.0.1:8765"
DC_ID = "DC"
DC_NAME = "库房"
DC_LNG = 116.568327
DC_LAT = 40.082845


def get_json(path: str):
    url = f"{BASE_URL}{path}"
    with urllib.request.urlopen(url, timeout=120) as resp:
        return json.loads(resp.read().decode("utf-8"))


def to_rows(items):
    if not items:
        return []
    keys = []
    seen = set()
    for row in items:
        if isinstance(row, dict):
            for k in row.keys():
                if k not in seen:
                    seen.add(k)
                    keys.append(k)
    rows = [keys]
    for row in items:
        rows.append([row.get(k, "") if isinstance(row, dict) else "" for k in keys])
    return rows


def write_xlsx(path: str, sheets):
    wb = Workbook()
    default = wb.active
    wb.remove(default)
    for sheet_name, rows in sheets:
        ws = wb.create_sheet(title=sheet_name[:31] or "Sheet")
        for r in rows:
            ws.append(r)
    wb.save(path)


def write_txt(path: str, sections):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"导出时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        for title, rows in sections:
            f.write(f"==== {title} ====\n")
            for r in rows:
                f.write("\t".join(str(x) for x in r) + "\n")
            f.write("\n")


def haversine_km(lng1, lat1, lng2, lat2):
    r = 6371.0088
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def parse_cache_key(cache_key: str):
    try:
        left, right = str(cache_key).split("->", 1)
        lng1, lat1 = [float(x) for x in left.split(",")]
        lng2, lat2 = [float(x) for x in right.split(",")]
        return lng1, lat1, lng2, lat2
    except Exception:
        return None


def load_distance_cache():
    # 复用后端同一连接，避免重复写配置
    import sys
    tools_dir = os.path.join(PROJECT_ROOT, "tools")
    if tools_dir not in sys.path:
        sys.path.insert(0, tools_dir)
    import ga_backend_server as g

    rows = []
    with g.mysql_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT cache_key, distance_km, duration_minutes, updated_at
                FROM {g.AMAP_DISTANCE_TABLE}
                ORDER BY cache_key
                """
            )
            rows = cursor.fetchall() or []
    return rows


def main():
    os.makedirs(PROJECT_ROOT, exist_ok=True)

    # 1) 双表货量全量
    load = get_json("/store-wave-load-resolved/list?limit=200000")
    clean = get_json("/clean-cargo-raw/list?limit=200000")
    load_items = load.get("items", [])
    clean_items = clean.get("items", [])

    huo_xlsx = os.path.join(PROJECT_ROOT, "huo.xlsx")
    huo_txt = os.path.join(PROJECT_ROOT, "huo.txt")

    load_rows = to_rows(load_items)
    clean_rows = to_rows(clean_items)
    write_xlsx(
        huo_xlsx,
        [
            ("store_wave_load_resolved", load_rows),
            ("wms_cargo_raw_clean_snapshot", clean_rows),
        ],
    )
    write_txt(
        huo_txt,
        [
            ("store_wave_load_resolved", load_rows),
            ("wms_cargo_raw_clean_snapshot", clean_rows),
        ],
    )

    # 2) 点位与点间距离
    points_resp = get_json("/stores/points")
    points = points_resp.get("items", [])
    dc_point = {
        "store_id": DC_ID,
        "store_name": DC_NAME,
        "lng": DC_LNG,
        "lat": DC_LAT,
    }
    points_with_dc = [dc_point, *points]
    points_rows = to_rows(points_with_dc)

    # 映射坐标到店铺
    coord_to_store = {}
    for p in points_with_dc:
        try:
            key = f"{float(p.get('lng')):.6f},{float(p.get('lat')):.6f}"
            coord_to_store[key] = str(p.get("store_id") or "")
        except Exception:
            pass

    # 高德缓存表（若有）
    cache_rows = load_distance_cache()
    cache_export = [[
        "cache_key",
        "from_store_id",
        "to_store_id",
        "from_lng",
        "from_lat",
        "to_lng",
        "to_lat",
        "distance_km",
        "duration_minutes",
        "updated_at",
    ]]
    for row in cache_rows:
        parsed = parse_cache_key(row.get("cache_key", ""))
        if not parsed:
            continue
        lng1, lat1, lng2, lat2 = parsed
        k1 = f"{lng1:.6f},{lat1:.6f}"
        k2 = f"{lng2:.6f},{lat2:.6f}"
        cache_export.append(
            [
                row.get("cache_key", ""),
                coord_to_store.get(k1, ""),
                coord_to_store.get(k2, ""),
                lng1,
                lat1,
                lng2,
                lat2,
                row.get("distance_km", ""),
                row.get("duration_minutes", ""),
                str(row.get("updated_at", "")),
            ]
        )

    # 全点对直线距离（完整覆盖）
    pair_rows = [[
        "from_store_id",
        "to_store_id",
        "from_lng",
        "from_lat",
        "to_lng",
        "to_lat",
        "straight_distance_km",
    ]]
    for i, a in enumerate(points_with_dc):
        try:
            alng = float(a.get("lng"))
            alat = float(a.get("lat"))
        except Exception:
            continue
        for j, b in enumerate(points_with_dc):
            if i == j:
                continue
            try:
                blng = float(b.get("lng"))
                blat = float(b.get("lat"))
            except Exception:
                continue
            pair_rows.append(
                [
                    str(a.get("store_id", "")),
                    str(b.get("store_id", "")),
                    alng,
                    alat,
                    blng,
                    blat,
                    round(haversine_km(alng, alat, blng, blat), 6),
                ]
            )

    lu_xlsx = os.path.join(PROJECT_ROOT, "lu.xlsx")
    lu_txt = os.path.join(PROJECT_ROOT, "lu.txt")
    write_xlsx(
        lu_xlsx,
        [
            ("points", points_rows),
            ("amap_distance_cache", cache_export),
            ("all_pairs_straight_km", pair_rows),
        ],
    )
    write_txt(
        lu_txt,
        [
            ("points", points_rows),
            ("amap_distance_cache", cache_export),
            ("all_pairs_straight_km", pair_rows),
        ],
    )

    print("ok")
    print(f"huo.xlsx={huo_xlsx}")
    print(f"huo.txt={huo_txt}")
    print(f"lu.xlsx={lu_xlsx}")
    print(f"lu.txt={lu_txt}")
    print(f"store_wave_load_resolved_rows={len(load_items)}")
    print(f"wms_cargo_raw_clean_snapshot_rows={len(clean_items)}")
    print(f"points_rows={len(points_with_dc)}")
    print(f"amap_cache_rows={max(0, len(cache_export)-1)}")
    print(f"all_pair_rows={max(0, len(pair_rows)-1)}")


if __name__ == "__main__":
    main()
