from __future__ import annotations

import json
import sys
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

CALC_VERSION = "route_spatial_rules_v1"


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


def ensure_table(c):
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS rengong_route_spatial_rules (
          id BIGINT NOT NULL AUTO_INCREMENT,
          route_type VARCHAR(64) NOT NULL,
          city_group VARCHAR(64) NOT NULL,
          forbidden_mix_with JSON NULL,
          rule_level VARCHAR(16) NOT NULL,
          rule_desc VARCHAR(255) NULL,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_calc_version (calc_version),
          KEY idx_route_type (route_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def main():
    rules = [
        {
            "route_type": "周边城市",
            "city_group": "承德",
            "forbidden_mix_with": ["张家口"],
            "rule_level": "hard",
            "rule_desc": "人工调度中不同周边城市从未混线，属于强隔离约束",
            "calc_version": CALC_VERSION,
        },
        {
            "route_type": "周边城市",
            "city_group": "张家口",
            "forbidden_mix_with": ["承德"],
            "rule_level": "hard",
            "rule_desc": "人工调度中不同周边城市从未混线，属于强隔离约束",
            "calc_version": CALC_VERSION,
        },
    ]

    with mysql_conn() as conn:
        with conn.cursor() as c:
            ensure_table(c)
            c.execute(
                "DELETE FROM rengong_route_spatial_rules WHERE calc_version=%s",
                (CALC_VERSION,),
            )
            sql = """
                INSERT INTO rengong_route_spatial_rules (
                  route_type, city_group, forbidden_mix_with,
                  rule_level, rule_desc, calc_version
                ) VALUES (%s,%s,%s,%s,%s,%s)
            """
            for r in rules:
                c.execute(
                    sql,
                    (
                        r["route_type"],
                        r["city_group"],
                        json.dumps(r["forbidden_mix_with"], ensure_ascii=False),
                        r["rule_level"],
                        r["rule_desc"],
                        r["calc_version"],
                    ),
                )

    print(f"written_count={len(rules)}")
    print("rules=")
    for r in rules:
        print(json.dumps(r, ensure_ascii=False))


if __name__ == "__main__":
    main()
