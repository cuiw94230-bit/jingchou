"""门店坐标同步脚本：从远程库/地理编码同步门店经纬度。"""`n`nimport json
import os
import urllib.parse
import urllib.request

import pymysql


AMAP_KEY = os.getenv("WHALE_AMAP_KEY", "3ba73c0e0906dcbe77eeb85f3a5c343d")

REMOTE = {
    "host": os.getenv("WHALE_REMOTE_DB_HOST", "8.148.247.60"),
    "port": int(os.getenv("WHALE_REMOTE_DB_PORT", "3306")),
    "user": os.getenv("WHALE_REMOTE_DB_USER", "root"),
    "password": os.getenv("WHALE_REMOTE_DB_PASSWORD", "Zjl@19930607"),
    "database": os.getenv("WHALE_REMOTE_DB_NAME", "ruoyi_vue"),
}
LOCAL = {
    "host": os.getenv("WHALE_LOCAL_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("WHALE_LOCAL_DB_PORT", "3306")),
    "user": os.getenv("WHALE_LOCAL_DB_USER", "root"),
    "password": os.getenv("WHALE_LOCAL_DB_PASSWORD", "105080zh"),
    "database": os.getenv("WHALE_LOCAL_DB_NAME", "whale_scheduler"),
}


def geocode(addr, city=""):
    if not addr:
        return None
    query = {"key": AMAP_KEY, "address": addr}
    if city:
        query["city"] = city
    url = "https://restapi.amap.com/v3/geocode/geo?" + urllib.parse.urlencode(query)
    with urllib.request.urlopen(url, timeout=15) as resp:
        payload = json.loads(resp.read().decode("utf-8", "ignore"))
    if str(payload.get("status")) != "1":
        return None
    geocodes = payload.get("geocodes") or []
    if not geocodes:
        return None
    location = str(geocodes[0].get("location") or "")
    if "," not in location:
        return None
    lng, lat = location.split(",", 1)
    try:
        return float(lng), float(lat)
    except Exception:
        return None


def valid_coord(lng, lat):
    try:
        return float(lng) != 0 and float(lat) != 0
    except Exception:
        return False


def main():
    remote = pymysql.connect(
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        **REMOTE,
    )
    local = pymysql.connect(
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
        **LOCAL,
    )
    rc = remote.cursor()
    lc = local.cursor()

    lc.execute("SELECT shop_code,shop_name,district,lng,lat FROM C_SHOP_MAIN ORDER BY shop_code")
    local_rows = lc.fetchall()
    local_codes = [row["shop_code"] for row in local_rows]
    if not local_codes:
        print("Local C_SHOP_MAIN is empty, nothing to sync.")
        return

    placeholders = ",".join(["%s"] * len(local_codes))
    rc.execute(
        f"SELECT ShopCode,ShopName,ShopAddress,Lng,Lat FROM C_SHOP_MAIN WHERE ShopCode IN ({placeholders})",
        local_codes,
    )
    remote_rows = rc.fetchall()
    remote_by_code = {str(row["ShopCode"]).strip(): row for row in remote_rows}

    # 1) detailed_address for all local stores (code match)
    detailed_filled = 0
    for row in local_rows:
        code = row["shop_code"]
        remote_row = remote_by_code.get(code)
        address = (remote_row.get("ShopAddress") if remote_row else None) or ""
        lc.execute("UPDATE C_SHOP_MAIN SET detailed_address=%s WHERE shop_code=%s", (address, code))
        if address:
            detailed_filled += 1

    # 2) fill missing coords from remote first, then AMap geocode
    lc.execute("SELECT shop_code,shop_name,district,detailed_address,lng,lat FROM C_SHOP_MAIN WHERE lng=0 OR lat=0")
    missing_rows = lc.fetchall()

    from_remote = 0
    from_amap = 0
    still_missing = []
    for row in missing_rows:
        code = row["shop_code"]
        remote_row = remote_by_code.get(code)
        if remote_row and valid_coord(remote_row.get("Lng"), remote_row.get("Lat")):
            lc.execute(
                "UPDATE C_SHOP_MAIN SET lng=%s, lat=%s WHERE shop_code=%s",
                (float(remote_row["Lng"]), float(remote_row["Lat"]), code),
            )
            from_remote += 1
            continue

        addr = (row.get("detailed_address") or "").strip()
        name = (row.get("shop_name") or "").strip()
        district = (row.get("district") or "").strip()
        tries = [addr, f"{district}{name}".strip(), name]
        coord = None
        for candidate in tries:
            if not candidate:
                continue
            coord = geocode(candidate, city="北京市" if ("北京" in candidate or district) else "")
            if coord:
                break
        if coord:
            lc.execute("UPDATE C_SHOP_MAIN SET lng=%s, lat=%s WHERE shop_code=%s", (coord[0], coord[1], code))
            from_amap += 1
        else:
            still_missing.append((code, name))

    # 3) ensure detailed_address non-empty (fallback to name)
    lc.execute("UPDATE C_SHOP_MAIN SET detailed_address=shop_name WHERE detailed_address IS NULL OR detailed_address=''")
    detailed_name_fallback = lc.rowcount

    lc.execute("SELECT COUNT(*) AS c FROM C_SHOP_MAIN")
    total = lc.fetchone()["c"]
    lc.execute("SELECT COUNT(*) AS c FROM C_SHOP_MAIN WHERE detailed_address IS NOT NULL AND detailed_address<>''")
    detailed_non_empty = lc.fetchone()["c"]
    lc.execute("SELECT COUNT(*) AS c FROM C_SHOP_MAIN WHERE lng<>0 AND lat<>0")
    coord_non_zero = lc.fetchone()["c"]

    print("Sync completed.")
    print(f"total={total}")
    print(f"detailed_filled_from_remote={detailed_filled}")
    print(f"coord_filled_from_remote={from_remote}")
    print(f"coord_filled_from_amap={from_amap}")
    print(f"detailed_fallback_to_name={detailed_name_fallback}")
    print(f"detailed_non_empty={detailed_non_empty}")
    print(f"coord_non_zero={coord_non_zero}")
    print(f"still_missing={len(still_missing)}")
    for code, name in still_missing:
        print(f"- {code}\t{name}")

    rc.close()
    lc.close()
    remote.close()
    local.close()


if __name__ == "__main__":
    main()
