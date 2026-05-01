"""高德距离矩阵刷新脚本：刷新 store_distance_matrix 的真实路网距离。"""`n`nimport argparse
import os
import time
from datetime import datetime
from typing import Dict, List, Tuple

import pymysql
import requests


PROJECT_ROOT = r"C:\Users\laoj0\Desktop\一二技术\城市便利店调度系统"
DEFAULT_DB = {
    "host": os.getenv("WHALE_DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("WHALE_DB_PORT", "3306")),
    "user": os.getenv("WHALE_DB_USER", "root"),
    "password": os.getenv("WHALE_DB_PASSWORD", "105080zh"),
    "database": os.getenv("WHALE_DB_NAME", "whale_scheduler"),
}

AMAP_URL = "https://restapi.amap.com/v3/distance"
AMAP_KEY = os.getenv("AMAP_WEB_SERVICE_KEY", "3ba73c0e0906dcbe77eeb85f3a5c343d")
SOURCE_TAG = "amap_driving"

DC_ID = "DC"
DC_LNG = 116.568327
DC_LAT = 40.082845

TABLE_MAIN = "store_distance_matrix"
TABLE_NEW = "store_distance_matrix_new"
TABLE_OLD = "store_distance_matrix_old_20260417"
TABLE_BAK = "store_distance_matrix_bak_20260417"

CHUNK_SIZE = 25
QPS_SLEEP = 0.8
MAX_RETRY = 4

FAIL_LOG = os.path.join(PROJECT_ROOT, "tools", f"failed_batches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")


def now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now()}] {msg}", flush=True)


def db_conn():
    return pymysql.connect(
        host=DEFAULT_DB["host"],
        port=DEFAULT_DB["port"],
        user=DEFAULT_DB["user"],
        password=DEFAULT_DB["password"],
        database=DEFAULT_DB["database"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def detect_shop_geo_columns() -> Tuple[str, str]:
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SHOW COLUMNS FROM C_SHOP_MAIN")
            cols = {r["Field"] for r in cur.fetchall()}
    if "lng" in cols and "lat" in cols:
        return "lng", "lat"
    if "longitude" in cols and "latitude" in cols:
        return "longitude", "latitude"
    raise RuntimeError("C_SHOP_MAIN 未找到经纬度字段（既无 lng/lat，也无 longitude/latitude）")


def ensure_tables() -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            log(f"创建备份表 {TABLE_BAK}（如已存在则跳过）")
            cur.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_BAK} AS SELECT * FROM {TABLE_MAIN}")
            log(f"重建新表 {TABLE_NEW}")
            cur.execute(f"DROP TABLE IF EXISTS {TABLE_NEW}")
            cur.execute(f"CREATE TABLE {TABLE_NEW} LIKE {TABLE_MAIN}")
            try:
                cur.execute(f"ALTER TABLE {TABLE_NEW} ADD UNIQUE KEY uk_from_to (from_store_id, to_store_id)")
            except Exception:
                pass
            cur.execute(f"INSERT INTO {TABLE_NEW} SELECT * FROM {TABLE_MAIN}")
    log("准备完成：备份表/新表已就绪")


def load_nodes() -> Dict[str, Tuple[float, float]]:
    lng_col, lat_col = detect_shop_geo_columns()
    nodes: Dict[str, Tuple[float, float]] = {DC_ID: (DC_LNG, DC_LAT)}
    with db_conn() as conn:
        with conn.cursor() as cur:
            sql = f"""
            SELECT shop_code, {lng_col} AS lng, {lat_col} AS lat
            FROM C_SHOP_MAIN
            WHERE shop_code IS NOT NULL
              AND {lng_col} IS NOT NULL
              AND {lat_col} IS NOT NULL
            """
            cur.execute(sql)
            for row in cur.fetchall():
                sid = str(row["shop_code"]).strip()
                if not sid:
                    continue
                try:
                    lng = float(row["lng"])
                    lat = float(row["lat"])
                except Exception:
                    continue
                nodes[sid] = (lng, lat)
    log(f"节点加载完成: {len(nodes)}（含 DC）")
    return nodes


def chunked(arr: List[str], size: int):
    for i in range(0, len(arr), size):
        yield arr[i:i + size]


def call_amap(origins: List[Tuple[float, float]], destination: Tuple[float, float]):
    params = {
        "key": AMAP_KEY,
        "origins": "|".join([f"{x},{y}" for x, y in origins]),
        "destination": f"{destination[0]},{destination[1]}",
        "type": 1,
        "strategy": 0,
    }
    for attempt in range(1, MAX_RETRY + 1):
        try:
            resp = requests.get(AMAP_URL, params=params, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") == "1" and isinstance(data.get("results"), list):
                return data["results"]
        except Exception:
            pass
        time.sleep(0.8 * attempt)
    return None


def append_failed(to_id: str, batch_ids: List[str], reason: str) -> None:
    line = f"{now()} | to={to_id} | size={len(batch_ids)} | reason={reason} | from={','.join(batch_ids)}\n"
    with open(FAIL_LOG, "a", encoding="utf-8") as f:
        f.write(line)


def upsert_rows(rows: List[Tuple[str, str, float, float, str]]) -> None:
    if not rows:
        return
    sql = f"""
    INSERT INTO {TABLE_NEW}
    (from_store_id, to_store_id, distance_km, duration_minutes, source, updated_at)
    VALUES (%s, %s, %s, %s, %s, NOW())
    ON DUPLICATE KEY UPDATE
      distance_km = VALUES(distance_km),
      duration_minutes = VALUES(duration_minutes),
      source = VALUES(source),
      updated_at = NOW()
    """
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(sql, rows)


def refresh_matrix(nodes: Dict[str, Tuple[float, float]]) -> None:
    ids = list(nodes.keys())
    total_directed_pairs = len(ids) * (len(ids) - 1)
    processed = 0
    failed_batches = 0

    log(f"开始刷新真实驾车距离，目标有向对数: {total_directed_pairs}")
    log(f"失败批次日志: {FAIL_LOG}")

    for to_id in ids:
        destination = nodes[to_id]
        from_ids = [x for x in ids if x != to_id]
        for batch_ids in chunked(from_ids, CHUNK_SIZE):
            origins = [nodes[x] for x in batch_ids]
            results = call_amap(origins, destination)
            if not results:
                failed_batches += 1
                append_failed(to_id, batch_ids, "empty_results")
                continue
            if len(results) != len(batch_ids):
                failed_batches += 1
                append_failed(to_id, batch_ids, f"result_size_mismatch:{len(results)}")
                continue

            rows = []
            for i, from_id in enumerate(batch_ids):
                item = results[i] or {}
                try:
                    dist_m = float(item.get("distance", 0) or 0)
                    dur_s = float(item.get("duration", 0) or 0)
                except Exception:
                    dist_m = 0
                    dur_s = 0
                if dist_m <= 0 or dur_s <= 0:
                    continue
                rows.append(
                    (
                        from_id,
                        to_id,
                        round(dist_m / 1000.0, 4),
                        round(dur_s / 60.0, 2),
                        SOURCE_TAG,
                    )
                )
            upsert_rows(rows)
            processed += len(batch_ids)

            if processed % 500 == 0:
                log(f"刷新进度: {processed}/{total_directed_pairs}")
            time.sleep(QPS_SLEEP)

    log(f"刷新完成: processed={processed}, failed_batches={failed_batches}")


def validate_matrix(nodes: Dict[str, Tuple[float, float]]) -> None:
    ids = list(nodes.keys())
    expected = len(ids) * (len(ids) - 1)
    with db_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) c FROM {TABLE_NEW}")
            total = cur.fetchone()["c"]
            cur.execute(
                f"SELECT COUNT(*) c FROM {TABLE_NEW} "
                "WHERE distance_km IS NULL OR distance_km<=0 OR duration_minutes IS NULL OR duration_minutes<=0"
            )
            bad = cur.fetchone()["c"]
            cur.execute(f"SELECT source, COUNT(*) c FROM {TABLE_NEW} GROUP BY source ORDER BY c DESC")
            src = cur.fetchall()

            placeholders = ",".join(["%s"] * len(ids))
            sql_missing = f"""
            SELECT COUNT(*) AS c
            FROM (
              SELECT a.node_id AS f, b.node_id AS t
              FROM (
                SELECT %s AS node_id
                UNION
                SELECT shop_code AS node_id FROM C_SHOP_MAIN WHERE shop_code IS NOT NULL
              ) a
              CROSS JOIN (
                SELECT %s AS node_id
                UNION
                SELECT shop_code AS node_id FROM C_SHOP_MAIN WHERE shop_code IS NOT NULL
              ) b
              WHERE a.node_id <> b.node_id
            ) p
            LEFT JOIN {TABLE_NEW} m
              ON m.from_store_id = p.f AND m.to_store_id = p.t
            WHERE m.from_store_id IS NULL
            """
            cur.execute(sql_missing, (DC_ID, DC_ID))
            missing = cur.fetchone()["c"]

    log(f"校验: 期望有向对={expected}, 表总行={total}, 异常值={bad}, 缺失对={missing}")
    log(f"source 分布: {src}")


def switch_tables() -> None:
    with db_conn() as conn:
        with conn.cursor() as cur:
            log(f"执行切换: {TABLE_MAIN} -> {TABLE_OLD}, {TABLE_NEW} -> {TABLE_MAIN}")
            cur.execute(
                f"RENAME TABLE {TABLE_MAIN} TO {TABLE_OLD}, {TABLE_NEW} TO {TABLE_MAIN}"
            )
    log("切换完成")


def main():
    parser = argparse.ArgumentParser(description="刷新真实驾驶距离到 store_distance_matrix")
    parser.add_argument("--prepare", action="store_true", help="创建备份表与新表")
    parser.add_argument("--refresh", action="store_true", help="拉取高德并写入新表")
    parser.add_argument("--validate", action="store_true", help="校验新表数据")
    parser.add_argument("--switch", action="store_true", help="原子切换新表为正式表")
    args = parser.parse_args()

    if not AMAP_KEY:
        raise RuntimeError("AMAP_WEB_SERVICE_KEY 为空，无法拉取真实驾驶距离")

    if args.prepare:
        ensure_tables()

    nodes = None
    if args.refresh or args.validate:
        nodes = load_nodes()

    if args.refresh:
        refresh_matrix(nodes)
    if args.validate:
        validate_matrix(nodes)
    if args.switch:
        switch_tables()

    if not (args.prepare or args.refresh or args.validate or args.switch):
        parser.print_help()


if __name__ == "__main__":
    main()
