import pymysql
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings


def main():
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
            c.execute("SELECT COUNT(1) c FROM store_distance_matrix")
            print("rows", c.fetchone()["c"])

            c.execute(
                "SELECT COUNT(1) c FROM store_distance_matrix WHERE from_store_id='DC' OR to_store_id='DC'"
            )
            print("with_DC", c.fetchone()["c"])

            c.execute(
                """
                SELECT from_store_id, COUNT(1) c
                FROM store_distance_matrix
                GROUP BY from_store_id
                ORDER BY c DESC
                LIMIT 15
                """
            )
            print("top_from_store")
            for row in c.fetchall() or []:
                print(row)


if __name__ == "__main__":
    main()
