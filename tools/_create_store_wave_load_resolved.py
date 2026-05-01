# 建表工具：创建 store_wave_load_resolved，供真实调度/推演统一读取门店货量。`r`nimport os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import pymysql
from config import settings

CREATE_SQL = '''
CREATE TABLE IF NOT EXISTS store_wave_load_resolved (
  shop_code VARCHAR(32) NOT NULL PRIMARY KEY,
  wave_belongs VARCHAR(32) NULL,
  wave1_load DECIMAL(14,6) NOT NULL DEFAULT 0,
  wave2_load DECIMAL(14,6) NOT NULL DEFAULT 0,
  wave3_load DECIMAL(14,6) NOT NULL DEFAULT 0,
  wave4_load DECIMAL(14,6) NOT NULL DEFAULT 0,
  total_resolved_load DECIMAL(14,6) NOT NULL DEFAULT 0,
  source_wave1_base DECIMAL(14,6) NOT NULL DEFAULT 0,
  source_wave2_base DECIMAL(14,6) NOT NULL DEFAULT 0,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_swlr_wave_belongs (wave_belongs)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
'''

TRUNC_SQL = 'TRUNCATE TABLE store_wave_load_resolved;'

INSERT_SQL = '''
INSERT INTO store_wave_load_resolved (
  shop_code, wave_belongs, wave1_load, wave2_load, wave3_load, wave4_load,
  total_resolved_load, source_wave1_base, source_wave2_base
)
SELECT
  t.shop_code,
  t.wave_belongs,
  CASE
    WHEN t.norm_belongs = '2' THEN 0
    WHEN FIND_IN_SET('1', t.norm_belongs) > 0 OR FIND_IN_SET('2', t.norm_belongs) > 0 THEN t.base1
    ELSE 0
  END AS wave1_load,
  CASE
    WHEN t.norm_belongs = '2' THEN (t.base1 + t.base2)
    WHEN FIND_IN_SET('2', t.norm_belongs) > 0 THEN t.base2
    ELSE 0
  END AS wave2_load,
  CASE
    WHEN FIND_IN_SET('3', t.norm_belongs) > 0 THEN (t.base1 + t.base2)
    ELSE 0
  END AS wave3_load,
  CASE
    WHEN FIND_IN_SET('4', t.norm_belongs) > 0 THEN (t.base1 + t.base2)
    ELSE 0
  END AS wave4_load,
  (
    CASE
      WHEN t.norm_belongs = '2' THEN 0
      WHEN FIND_IN_SET('1', t.norm_belongs) > 0 OR FIND_IN_SET('2', t.norm_belongs) > 0 THEN t.base1
      ELSE 0
    END
    +
    CASE
      WHEN t.norm_belongs = '2' THEN (t.base1 + t.base2)
      WHEN FIND_IN_SET('2', t.norm_belongs) > 0 THEN t.base2
      ELSE 0
    END
    +
    CASE WHEN FIND_IN_SET('3', t.norm_belongs) > 0 THEN (t.base1 + t.base2) ELSE 0 END
    +
    CASE WHEN FIND_IN_SET('4', t.norm_belongs) > 0 THEN (t.base1 + t.base2) ELSE 0 END
  ) AS total_resolved_load,
  t.base1,
  t.base2
FROM (
  SELECT
    l.shop_code,
    COALESCE(m.wave_belongs, '') AS wave_belongs,
    REPLACE(REPLACE(REPLACE(COALESCE(m.wave_belongs, ''), '，', ','), '、', ','), ' ', '') AS norm_belongs,
    COALESCE(l.wave1_total_load, 0) AS base1,
    COALESCE(l.wave2_total_load, 0) AS base2
  FROM local_store_total_load l
  LEFT JOIN c_shop_main m ON m.shop_code = l.shop_code
) t;
'''

conn = pymysql.connect(host=settings.MYSQL_HOST, port=int(settings.MYSQL_PORT), user=settings.MYSQL_USER, password=settings.MYSQL_PASSWORD, database=settings.MYSQL_DATABASE, charset='utf8mb4', autocommit=False)
cur = conn.cursor()
try:
    cur.execute(CREATE_SQL)
    cur.execute(TRUNC_SQL)
    cur.execute(INSERT_SQL)
    conn.commit()

    cur.execute('SELECT COUNT(*) FROM store_wave_load_resolved')
    total = cur.fetchone()[0]
    cur.execute("SELECT shop_code,wave_belongs,wave1_load,wave2_load,wave3_load,wave4_load FROM store_wave_load_resolved WHERE wave_belongs='2' ORDER BY shop_code LIMIT 5")
    sample = cur.fetchall()
    print('table=store_wave_load_resolved')
    print('rows=' + str(total))
    print('sample_wave_belongs_2=')
    for r in sample:
        print(r)
finally:
    cur.close()
    conn.close()
