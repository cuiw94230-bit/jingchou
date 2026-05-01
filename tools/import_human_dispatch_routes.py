# 人工调度线路导入脚本：从 Excel 导入人工方案并生成行级哈希。`r`n# -*- coding: utf-8 -*-
import os
import sys
import argparse
import hashlib
from datetime import datetime

import pandas as pd
import pymysql

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings  # noqa


REQUIRED_EXCEL_COLUMNS = [
    "送货日期",
    "公司",
    "司机",
    "车牌号",
    "线路号",
    "线路名称",
    "序号",
    "店铺代码",
    "店铺名称",
    "常温周转箱",
    "常温纸箱",
    "常温整箱水",
    "冷冻包",
    "冷冻纸箱",
    "冷藏筐",
    "冷藏纸箱",
]

COLUMN_MAP = {
    "delivery_date": "送货日期",
    "company_name": "公司",
    "driver_name": "司机",
    "vehicle_id": "车牌号",
    "route_id": "线路号",
    "route_name": "线路名称",
    "stop_sequence": "序号",
    "store_id": "店铺代码",
    "store_name": "店铺名称",
    "ambient_turnover_boxes": "常温周转箱",
    "ambient_cartons": "常温纸箱",
    "ambient_full_cases": "常温整箱水",
    "frozen_bags": "冷冻包",
    "frozen_cartons": "冷冻纸箱",
    "cold_bins": "冷藏筐",
    "cold_cartons": "冷藏纸箱",
}

INSERT_SQL = """
INSERT INTO human_dispatch_routes (
    import_batch_id,
    source_sheet,
    delivery_date,
    company_name,
    driver_name,
    vehicle_id,
    route_id,
    route_name,
    stop_sequence,
    store_id,
    store_name,
    ambient_turnover_boxes,
    ambient_cartons,
    ambient_full_cases,
    frozen_bags,
    frozen_cartons,
    cold_bins,
    cold_cartons,
    load_ratio,
    record_hash,
    source_file,
    source_row_no
) VALUES (
    %(import_batch_id)s,
    %(source_sheet)s,
    %(delivery_date)s,
    %(company_name)s,
    %(driver_name)s,
    %(vehicle_id)s,
    %(route_id)s,
    %(route_name)s,
    %(stop_sequence)s,
    %(store_id)s,
    %(store_name)s,
    %(ambient_turnover_boxes)s,
    %(ambient_cartons)s,
    %(ambient_full_cases)s,
    %(frozen_bags)s,
    %(frozen_cartons)s,
    %(cold_bins)s,
    %(cold_cartons)s,
    %(load_ratio)s,
    %(record_hash)s,
    %(source_file)s,
    %(source_row_no)s
)
"""


def db_conn():
    return pymysql.connect(
        host=settings.MYSQL_HOST,
        port=int(settings.MYSQL_PORT),
        user=settings.MYSQL_USER,
        password=settings.MYSQL_PASSWORD,
        database=settings.MYSQL_DATABASE,
        charset="utf8mb4",
        autocommit=False,
    )


def norm_col(col):
    return str(col).replace("\ufeff", "").strip()


def is_blank(v):
    if pd.isna(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False


def to_str(v):
    if pd.isna(v):
        return ""
    return str(v).strip()


def to_int(v):
    if pd.isna(v):
        return 0
    if isinstance(v, str):
        v = v.strip()
        if v == "":
            return 0
    try:
        return int(float(v))
    except Exception:
        return 0


def to_date(v):
    if pd.isna(v):
        return None
    if isinstance(v, datetime):
        return v.date()

    s = str(v).strip()
    if not s:
        return None

    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            pass

    try:
        return pd.to_datetime(s).date()
    except Exception:
        return None


def validate_columns(df, sheet_name):
    actual = [norm_col(c) for c in df.columns]
    missing = [c for c in REQUIRED_EXCEL_COLUMNS if c not in actual]
    if missing:
        raise ValueError(f"sheet={sheet_name} 缺少列: {missing}")


def make_hash(row):
    key = "|".join([
        str(row["delivery_date"]),
        str(row["vehicle_id"]),
        str(row["route_id"]),
        str(row["stop_sequence"]),
        str(row["store_id"]),
    ])
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def build_row(src, import_batch_id, source_sheet, source_file, source_row_no):
    delivery_date = to_date(src[COLUMN_MAP["delivery_date"]])
    route_raw = src[COLUMN_MAP["route_id"]]
    seq_raw = src[COLUMN_MAP["stop_sequence"]]
    store_raw = src[COLUMN_MAP["store_id"]]

    route_id = to_int(route_raw)
    stop_sequence = to_int(seq_raw)
    store_id = to_int(store_raw)
    vehicle_id = to_str(src[COLUMN_MAP["vehicle_id"]])

    # 非有效门店行直接跳过
    if (
        delivery_date is None
        or is_blank(route_raw)
        or is_blank(seq_raw)
        or is_blank(store_raw)
        or route_id == 0
        or stop_sequence == 0
        or store_id == 0
        or vehicle_id == ""
    ):
        return None

    row = {
        "import_batch_id": import_batch_id,
        "source_sheet": source_sheet,
        "delivery_date": delivery_date,
        "company_name": to_str(src[COLUMN_MAP["company_name"]]) or None,
        "driver_name": to_str(src[COLUMN_MAP["driver_name"]]) or None,
        "vehicle_id": vehicle_id,
        "route_id": route_id,
        "route_name": to_str(src[COLUMN_MAP["route_name"]]) or None,
        "stop_sequence": stop_sequence,
        "store_id": store_id,
        "store_name": to_str(src[COLUMN_MAP["store_name"]]) or None,
        "ambient_turnover_boxes": to_int(src[COLUMN_MAP["ambient_turnover_boxes"]]),
        "ambient_cartons": to_int(src[COLUMN_MAP["ambient_cartons"]]),
        "ambient_full_cases": to_int(src[COLUMN_MAP["ambient_full_cases"]]),
        "frozen_bags": to_int(src[COLUMN_MAP["frozen_bags"]]),
        "frozen_cartons": to_int(src[COLUMN_MAP["frozen_cartons"]]),
        "cold_bins": to_int(src[COLUMN_MAP["cold_bins"]]),
        "cold_cartons": to_int(src[COLUMN_MAP["cold_cartons"]]),
        "load_ratio": None,
        "record_hash": "",
        "source_file": source_file,
        "source_row_no": source_row_no,
    }

    row["record_hash"] = make_hash(row)
    return row


def import_one_sheet(cur, excel_path, sheet_name, import_batch_id):
    df = pd.read_excel(excel_path, sheet_name=sheet_name)
    df.columns = [norm_col(c) for c in df.columns]
    validate_columns(df, sheet_name)

    read_rows = 0
    inserted_rows = 0
    skipped_rows = 0
    failed_rows = 0

    for idx, src in df.iterrows():
        read_rows += 1
        source_row_no = idx + 2  # Excel 第1行表头

        try:
            row = build_row(
                src=src,
                import_batch_id=import_batch_id,
                source_sheet=sheet_name,
                source_file=os.path.basename(excel_path),
                source_row_no=source_row_no,
            )

            if row is None:
                skipped_rows += 1
                continue

            try:
                cur.execute(INSERT_SQL, row)
                inserted_rows += 1
            except pymysql.err.IntegrityError:
                skipped_rows += 1

        except Exception:
            failed_rows += 1

    return read_rows, inserted_rows, skipped_rows, failed_rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Excel 文件路径")
    parser.add_argument("--sheet", required=False, help="可选：只导入指定 sheet")
    args = parser.parse_args()

    excel_path = args.file
    if not os.path.exists(excel_path):
        raise FileNotFoundError(excel_path)

    xls = pd.ExcelFile(excel_path)
    target_sheets = [args.sheet] if args.sheet else list(xls.sheet_names)

    if args.sheet and args.sheet not in xls.sheet_names:
        raise ValueError(f"sheet 不存在: {args.sheet}")

    import_batch_id = int(datetime.now().timestamp() * 1000)

    total_read = 0
    total_inserted = 0
    total_skipped = 0
    total_failed = 0

    conn = db_conn()
    try:
        with conn.cursor() as cur:
            for sheet_name in target_sheets:
                read_rows, inserted_rows, skipped_rows, failed_rows = import_one_sheet(
                    cur, excel_path, sheet_name, import_batch_id
                )
                total_read += read_rows
                total_inserted += inserted_rows
                total_skipped += skipped_rows
                total_failed += failed_rows

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"import_batch_id={import_batch_id}")
    print(f"sheet_count={len(target_sheets)}")
    print(f"read_rows={total_read}")
    print(f"inserted_rows={total_inserted}")
    print(f"skipped_rows={total_skipped}")
    print(f"failed_rows={total_failed}")


if __name__ == "__main__":
    main()