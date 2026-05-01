# 数据库巡检脚本：列出表和关键字段，便于排查建表/字段缺失。`r`nimport os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pymysql
from config import settings

conn = pymysql.connect(host=settings.MYSQL_HOST, port=int(settings.MYSQL_PORT), user=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD, database=settings.MYSQL_DATABASE, charset='utf8mb4')
cur = conn.cursor()
cur.execute("SHOW TABLES")
print('TABLES:')
for (t,) in cur.fetchall():
    print(t)
print('--- COLUMNS with wave_belongs ---')
cur.execute("SELECT TABLE_NAME, COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=%s AND COLUMN_NAME IN ('wave_belongs','shop_code','wave1_total_load','wave2_total_load','total_load') ORDER BY TABLE_NAME,COLUMN_NAME", (settings.MYSQL_DATABASE,))
for row in cur.fetchall():
    print(row)
cur.close(); conn.close()
