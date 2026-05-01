import sys
import pymysql

sys.path.insert(0, r"C:\Users\laoj0\Desktop\一二技术\城市便利店调度系统")
sys.path.insert(0, r"C:\Users\laoj0\Desktop\一二技术\城市便利店调度系统\tools")

from config import settings
import fill_missing_amap_segments_weekday_dual_w1 as m


def main():
    missing, _ = m.build_missing_pairs()
    stores = sorted({s for a, b in missing for s in (a, b) if s != "DC"})
    conn = pymysql.connect(
        host=settings.MYSQL_HOST,
        port=int(settings.MYSQL_PORT),
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
        database=settings.MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
    )
    with conn:
        with conn.cursor() as c:
            if stores:
                ph = ",".join(["%s"] * len(stores))
                c.execute(f"SELECT shop_code FROM C_SHOP_MAIN WHERE shop_code IN ({ph})", stores)
                found = {str(r.get("shop_code") or "").strip() for r in (c.fetchall() or [])}
            else:
                found = set()
    absent = [s for s in stores if s not in found]
    print("stores", len(stores))
    print("found_in_C_SHOP_MAIN", len(found))
    print("absent", len(absent))
    print("absent_sample", absent[:80])


if __name__ == "__main__":
    main()

