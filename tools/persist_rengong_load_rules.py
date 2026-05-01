from __future__ import annotations

import json
import sys
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

RULE_SCOPE = "weekday_city_dual_w1"
CALC_VERSION = "load_rules_v1"


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


def ensure_table(cursor):
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS rengong_load_rules (
          id BIGINT NOT NULL AUTO_INCREMENT,
          rule_scope VARCHAR(64) NOT NULL,
          rule_type VARCHAR(64) NOT NULL,
          route_category VARCHAR(128) NULL,
          hard_load_limit DECIMAL(10,4) NULL,
          ignore_load_limit BOOLEAN NULL,
          vehicle_rule_type VARCHAR(64) NULL,
          preferred_vehicle_ids JSON NULL,
          allow_multi_trip BOOLEAN NULL,
          dirty_load_threshold DECIMAL(10,4) NULL,
          dirty_exclude_airport BOOLEAN NULL,
          rule_desc VARCHAR(500) NULL,
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          calc_version VARCHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
          updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          PRIMARY KEY (id),
          KEY idx_scope_version (rule_scope, calc_version),
          KEY idx_scope_type (rule_scope, rule_type)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )


def main():
    rules = [
        {
            "rule_scope": RULE_SCOPE,
            "rule_type": "normal",
            "route_category": "normal",
            "hard_load_limit": 1.25,
            "ignore_load_limit": False,
            "vehicle_rule_type": "normal",
            "preferred_vehicle_ids": None,
            "allow_multi_trip": False,
            "dirty_load_threshold": None,
            "dirty_exclude_airport": None,
            "rule_desc": "普通平日市内W1线路，装载硬上限1.25",
            "is_active": True,
            "calc_version": CALC_VERSION,
        },
        {
            "rule_scope": RULE_SCOPE,
            "rule_type": "airport",
            "route_category": "首都机场",
            "hard_load_limit": None,
            "ignore_load_limit": True,
            "vehicle_rule_type": "preferred",
            "preferred_vehicle_ids": ["京LJD975"],
            "allow_multi_trip": True,
            "dirty_load_threshold": None,
            "dirty_exclude_airport": True,
            "rule_desc": "首都机场线路不受装载上限限制，优先使用京LJD975，可多趟，核心约束为时间完成",
            "is_active": True,
            "calc_version": CALC_VERSION,
        },
        {
            "rule_scope": RULE_SCOPE,
            "rule_type": "dirty_filter",
            "route_category": None,
            "hard_load_limit": None,
            "ignore_load_limit": None,
            "vehicle_rule_type": "normal",
            "preferred_vehicle_ids": None,
            "allow_multi_trip": None,
            "dirty_load_threshold": 5,
            "dirty_exclude_airport": True,
            "rule_desc": "route_load_w1 > 5 且非首都机场线路判定为脏数据",
            "is_active": True,
            "calc_version": CALC_VERSION,
        },
    ]

    with mysql_conn() as conn:
        with conn.cursor() as c:
            ensure_table(c)
            c.execute(
                """
                DELETE FROM rengong_load_rules
                WHERE rule_scope=%s AND calc_version=%s
                """,
                (RULE_SCOPE, CALC_VERSION),
            )
            ins = """
                INSERT INTO rengong_load_rules (
                  rule_scope, rule_type, route_category, hard_load_limit, ignore_load_limit,
                  vehicle_rule_type, preferred_vehicle_ids, allow_multi_trip,
                  dirty_load_threshold, dirty_exclude_airport, rule_desc, is_active, calc_version
                ) VALUES (
                  %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
                )
            """
            for r in rules:
                c.execute(
                    ins,
                    (
                        r["rule_scope"],
                        r["rule_type"],
                        r["route_category"],
                        r["hard_load_limit"],
                        r["ignore_load_limit"],
                        r["vehicle_rule_type"],
                        None if r["preferred_vehicle_ids"] is None else json.dumps(r["preferred_vehicle_ids"], ensure_ascii=False),
                        r["allow_multi_trip"],
                        r["dirty_load_threshold"],
                        r["dirty_exclude_airport"],
                        r["rule_desc"],
                        r["is_active"],
                        r["calc_version"],
                    ),
                )

    print(f"written_rules={len(rules)}")
    print(f"rule_scope={RULE_SCOPE}")
    print(f"calc_version={CALC_VERSION}")


if __name__ == "__main__":
    main()
