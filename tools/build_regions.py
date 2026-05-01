"""区域构建脚本：按门店坐标与重叠规则生成运行区数据。"""`n`n# -*- coding: utf-8 -*-
import os
import sys
from collections import defaultdict, Counter
import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

OVERLAP_THRESHOLD = 0.6

def db_conn():
    return pymysql.connect(
        host=settings.MYSQL_HOST,
        port=int(settings.MYSQL_PORT),
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
        database=settings.MYSQL_DATABASE,
        charset="utf8mb4",
    )

def main():
    print("start")
    conn = db_conn()
    cur = conn.cursor()

    cur.execute("SELECT route_id, vehicle_id, store_id FROM human_dispatch_routes")
    rows = cur.fetchall()

    route_store = defaultdict(set)
    route_vehicle = {}

    for r in rows:
        route_id, vehicle_id, store_id = r
        route_store[route_id].add(store_id)
        route_vehicle[route_id] = vehicle_id

    visited = set()
    clusters = []

    for r1 in route_store:
        if r1 in visited:
            continue

        cluster = set([r1])
        visited.add(r1)

        for r2 in route_store:
            if r2 in visited:
                continue

            if route_vehicle[r1] != route_vehicle[r2]:
                continue

            s1 = route_store[r1]
            s2 = route_store[r2]

            inter = len(s1 & s2)
            union = len(s1 | s2)

            if union == 0:
                continue

            if inter / union >= OVERLAP_THRESHOLD:
                cluster.add(r2)
                visited.add(r2)

        clusters.append(cluster)

    cur.execute("DELETE FROM dispatch_regions")
    cur.execute("DELETE FROM store_region_map")

    region_id = 1
    store_region_counter = defaultdict(Counter)

    for cluster in clusters:
        cur.execute(
            "INSERT INTO dispatch_regions (id, region_name) VALUES (%s, %s)",
            (region_id, f'R{region_id}')
        )

        for route_id in cluster:
            for store_id in route_store[route_id]:
                store_region_counter[store_id][region_id] += 1

        region_id += 1

    for store_id, counter in store_region_counter.items():
        region_id, cnt = counter.most_common(1)[0]
        total = sum(counter.values())
        confidence = cnt / total

        cur.execute(
            "INSERT INTO store_region_map (store_id, region_id, confidence) VALUES (%s,%s,%s)",
            (store_id, region_id, confidence)
        )

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
