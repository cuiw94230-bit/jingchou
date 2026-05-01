# 临时规则核验脚本：用于导出或检查规则数据，后续建议纳入正式工具或归档。`r`nimport json
import pymysql
from pathlib import Path

env=Path(r"C:\Users\laoj0\Desktop\一二技术\城市便利店调度系统\.env")
cfg={}
for line in env.read_text(encoding='utf-8').splitlines():
    s=line.strip()
    if not s or s.startswith('#') or '=' not in s:
        continue
    k,v=s.split('=',1)
    cfg[k.strip()]=v.strip()

conn=pymysql.connect(
    host=cfg.get('WHALE_DB_HOST','127.0.0.1'),
    port=int(cfg.get('WHALE_DB_PORT','3306')),
    user=cfg.get('WHALE_DB_USER','root'),
    password=cfg.get('WHALE_DB_PASSWORD',''),
    database=cfg.get('WHALE_DB_NAME','whale_scheduler'),
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor,
    autocommit=True,
)
cur=conn.cursor()

for sql in [
    'DROP TEMPORARY TABLE IF EXISTS tmp_base_routes',
    'DROP TEMPORARY TABLE IF EXISTS tmp_store_stat',
    'DROP TEMPORARY TABLE IF EXISTS tmp_store_label',
    'DROP TEMPORARY TABLE IF EXISTS tmp_route_stat',
    'DROP TEMPORARY TABLE IF EXISTS tmp_route_stat_r1',
    'DROP TEMPORARY TABLE IF EXISTS tmp_route_stat_r2',
    'DROP TEMPORARY TABLE IF EXISTS tmp_route_label',
    'DROP TEMPORARY TABLE IF EXISTS tmp_pair_base',
    'DROP TEMPORARY TABLE IF EXISTS tmp_pair_stat',
]:
    cur.execute(sql)

cur.execute("""CREATE TEMPORARY TABLE tmp_base_routes AS
SELECT DISTINCT delivery_date, route_id, vehicle_id, store_id
FROM human_dispatch_routes
WHERE delivery_date IS NOT NULL AND route_id IS NOT NULL AND vehicle_id IS NOT NULL AND store_id IS NOT NULL""")

cur.execute("""CREATE TEMPORARY TABLE tmp_store_stat AS
SELECT delivery_date, store_id, COUNT(DISTINCT route_id) AS route_cnt, MIN(route_id) AS min_route_id, MAX(route_id) AS max_route_id
FROM tmp_base_routes
GROUP BY delivery_date, store_id""")

cur.execute("""CREATE TEMPORARY TABLE tmp_store_label AS
SELECT delivery_date, store_id, route_cnt, min_route_id, max_route_id,
CASE WHEN route_cnt=2 AND (max_route_id-min_route_id=100) THEN 1 ELSE 0 END AS is_dual_store
FROM tmp_store_stat""")

cur.execute("""CREATE TEMPORARY TABLE tmp_route_stat AS
SELECT b.delivery_date,b.route_id,COUNT(DISTINCT b.store_id) AS store_cnt,
COUNT(DISTINCT CASE WHEN s.is_dual_store=1 THEN b.store_id END) AS dual_store_cnt
FROM tmp_base_routes b
LEFT JOIN tmp_store_label s ON b.delivery_date=s.delivery_date AND b.store_id=s.store_id
GROUP BY b.delivery_date,b.route_id""")

cur.execute("""CREATE TEMPORARY TABLE tmp_route_label AS
SELECT delivery_date,route_id,store_cnt,dual_store_cnt,
CASE WHEN dual_store_cnt=store_cnt THEN 'dual_route' WHEN dual_store_cnt=0 THEN 'single_route' ELSE 'mixed_route' END AS route_type
FROM tmp_route_stat""")

cur.execute("""CREATE TEMPORARY TABLE tmp_pair_base AS
SELECT delivery_date,vehicle_id,COUNT(DISTINCT route_id) AS route_cnt,MIN(route_id) AS r1,MAX(route_id) AS r2
FROM tmp_base_routes
GROUP BY delivery_date,vehicle_id
HAVING COUNT(DISTINCT route_id)=2 AND (MAX(route_id)-MIN(route_id)=100)""")

cur.execute('CREATE TEMPORARY TABLE tmp_route_stat_r1 AS SELECT * FROM tmp_route_stat')
cur.execute('CREATE TEMPORARY TABLE tmp_route_stat_r2 AS SELECT * FROM tmp_route_stat')

cur.execute("""CREATE TEMPORARY TABLE tmp_pair_stat AS
SELECT p.delivery_date,p.vehicle_id,p.r1,p.r2,r1.store_cnt AS r1_store_cnt,r2.store_cnt AS r2_store_cnt,
CASE WHEN r2.store_cnt>r1.store_cnt THEN 'second_more' WHEN r2.store_cnt=r1.store_cnt THEN 'equal' ELSE 'abnormal' END AS pair_type
FROM tmp_pair_base p
LEFT JOIN tmp_route_stat_r1 r1 ON p.delivery_date=r1.delivery_date AND p.r1=r1.route_id
LEFT JOIN tmp_route_stat_r2 r2 ON p.delivery_date=r2.delivery_date AND p.r2=r2.route_id""")

cur.execute("SELECT delivery_date, route_id, store_cnt, dual_store_cnt, route_type FROM tmp_route_label WHERE route_type='mixed_route' ORDER BY delivery_date DESC, route_id")
mixed_rows = cur.fetchall()

cur.execute("SELECT delivery_date, vehicle_id, r1, r2, r1_store_cnt, r2_store_cnt, pair_type FROM tmp_pair_stat WHERE pair_type='abnormal' ORDER BY delivery_date DESC, vehicle_id")
abnormal_rows = cur.fetchall()

outdir=Path(r"C:\Users\laoj0\Desktop\一二技术\城市便利店调度系统\tools")
mixed_file=outdir/'mixed_route_all.json'
abnormal_file=outdir/'abnormal_pair_all.json'
mixed_file.write_text(json.dumps(mixed_rows,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
abnormal_file.write_text(json.dumps(abnormal_rows,ensure_ascii=False,indent=2,default=str),encoding='utf-8')

print('mixed_count=' + str(len(mixed_rows)))
print('abnormal_count=' + str(len(abnormal_rows)))
print('mixed_file=' + str(mixed_file))
print('abnormal_file=' + str(abnormal_file))

cur.close()
conn.close()
