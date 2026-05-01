"""
导出“平日市内双配波次一”逐日逐线路明细（只读分析）。

输出文件（UTF-8 BOM）:
1) route_level.csv  一行一条线路
2) stop_level.csv   一行一个停靠

规则:
- 仅 human_dispatch_routes 数据范围
- 仅平日（排除周日）
- 仅双配第一趟（同日同车存在 route_id=X 与 X+100，且当前取 X）
- 不调用外部 API，只使用本地缓存表 store_distance_matrix
- 不做推测补齐：缺失距离/时长/服务时间时直接标记缺失
"""

from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pymysql

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
from config import settings

OUTPUT_DIR = PROJECT_ROOT / "tools"
ROUTE_CSV = OUTPUT_DIR / "route_level.csv"
STOP_CSV = OUTPUT_DIR / "stop_level.csv"

DC_ID = "DC"


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


def _to_float(v):
    if v is None:
        return 0.0
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, Decimal):
        return float(v)
    s = str(v).strip()
    if not s:
        return 0.0
    try:
        return float(s)
    except Exception:
        return 0.0


def _to_int(v):
    return int(round(_to_float(v)))


def _hhmm_to_dt(delivery_date: str, hhmm: str):
    base = datetime.strptime(f"{delivery_date} {hhmm}", "%Y-%m-%d %H:%M")
    return base


def _minutes_to_hhmm(v):
    m = int(round(_to_float(v)))
    m = max(0, min(1439, m))
    return f"{m // 60:02d}:{m % 60:02d}"


def _normalize_hhmm(v: str):
    s = str(v or "").strip()
    if not s:
        return ""
    try:
        if len(s) >= 5 and s[2] == ":":
            hh = int(s[:2])
            mm = int(s[3:5])
            if 0 <= hh <= 23 and 0 <= mm <= 59:
                return f"{hh:02d}:{mm:02d}"
    except Exception:
        return ""
    return ""


def _fmt_dt(dt_obj):
    if not dt_obj:
        return ""
    return dt_obj.strftime("%Y-%m-%d %H:%M")


def _fmt_hhmm(dt_obj):
    if not dt_obj:
        return ""
    return dt_obj.strftime("%H:%M")


def _is_sunday(date_text: str):
    # Python: Monday=0 ... Sunday=6
    return datetime.strptime(date_text, "%Y-%m-%d").weekday() == 6


def load_route_rows():
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT
                  delivery_date,
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
                  cold_cartons
                FROM human_dispatch_routes
                ORDER BY delivery_date, vehicle_id, route_id, stop_sequence
                """
            )
            return c.fetchall() or []


def load_store_resolved():
    mapping = {}
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT
                  shop_code,wave_belongs,wave1_load,wave2_load,wave3_load,wave4_load,total_resolved_load,
                  first_wave_time,second_wave_time,arrival_time_w3,arrival_time_w4,arrival_time,updated_at
                FROM store_wave_load_resolved
                """
            )
            for row in c.fetchall() or []:
                code = str((row or {}).get("shop_code") or "").strip()
                if code:
                    mapping[code] = row
    return mapping


def load_wave1_time_map():
    mapping = {}
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT shop_code, first_wave_time
                FROM store_wave_timing_resolved
                """
            )
            for row in c.fetchall() or []:
                code = str((row or {}).get("shop_code") or "").strip()
                hhmm = _normalize_hhmm((row or {}).get("first_wave_time"))
                if code and hhmm:
                    mapping[code] = hhmm

            c.execute(
                """
                SELECT shop_code, first_wave_time
                FROM store_wave_load_resolved
                """
            )
            for row in c.fetchall() or []:
                code = str((row or {}).get("shop_code") or "").strip()
                hhmm = _normalize_hhmm((row or {}).get("first_wave_time"))
                if code and hhmm and code not in mapping:
                    mapping[code] = hhmm
    return mapping


def load_wave_config_w1():
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT wave_id, start_min, end_min, end_mode, created_at
                FROM dispatch_wave_result
                WHERE wave_id='W1'
                  AND start_min IS NOT NULL
                  AND end_min IS NOT NULL
                  AND end_mode IS NOT NULL
                ORDER BY created_at DESC
                LIMIT 1
                """
            )
            row = c.fetchone()
            if not row:
                raise RuntimeError(
                    "未找到W1波次配置来源（dispatch_wave_result.W1 的 start_min/end_min/end_mode）。"
                )
            return {
                "wave_id": str((row or {}).get("wave_id") or "").strip() or "W1",
                "start_time": _minutes_to_hhmm((row or {}).get("start_min")),
                "end_time": _minutes_to_hhmm((row or {}).get("end_min")),
                "cutoff_rule": str((row or {}).get("end_mode") or "").strip(),
                "source": "dispatch_wave_result",
            }


def load_profile_service():
    mapping = {}
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT shop_code, service_minutes
                FROM human_dispatch_solver_profile
                """
            )
            for row in c.fetchall() or []:
                code = str((row or {}).get("shop_code") or "").strip()
                if code:
                    val = (row or {}).get("service_minutes")
                    mapping[code] = None if val in (None, "") else _to_int(val)

            # 经验导出兜底：仅只读 C_SHOP_MAIN，不写入任何主表。
            # 规则：service_minutes 缺失 -> 15；difficulty 缺失 -> 1（仅读取留档，本脚本不参与时序计算）。
            c.execute(
                """
                SELECT shop_code, service_minutes, difficulty
                FROM C_SHOP_MAIN
                """
            )
            for row in c.fetchall() or []:
                code = str((row or {}).get("shop_code") or "").strip()
                if not code:
                    continue
                service_raw = (row or {}).get("service_minutes")
                _difficulty = (row or {}).get("difficulty")
                service_val = 15 if service_raw in (None, "") else _to_int(service_raw)
                if service_val <= 0:
                    service_val = 15
                if mapping.get(code) in (None, "", 0):
                    mapping[code] = service_val

    # 最终兜底，避免到店链路因服务时长空值中断
    for code, val in list(mapping.items()):
        if val in (None, "", 0):
            mapping[code] = 15
    return mapping


def load_vehicle_capacity():
    """
    车辆容量优先从 payload_json 取，取不到则为空。
    """
    mapping = {}
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                """
                SELECT plate_no, payload_json
                FROM wms_vehicle_snapshot
                """
            )
            for row in c.fetchall() or []:
                plate = str((row or {}).get("plate_no") or "").strip()
                payload = (row or {}).get("payload_json")
                if not plate:
                    continue
                capacity = None
                if payload:
                    try:
                        obj = json.loads(payload)
                        # 仅从真实 payload 字段读取，不做推测
                        for key in ("capacity", "load_capacity", "max_load", "volume_capacity"):
                            if key in obj and obj[key] not in (None, ""):
                                capacity = _to_float(obj[key])
                                break
                    except Exception:
                        capacity = None
                if capacity is not None and capacity > 0:
                    mapping[plate] = capacity
    return mapping


def load_distance_map(store_ids):
    if not store_ids:
        return {}
    ids = sorted(set([DC_ID] + list(store_ids)))
    placeholders = ",".join(["%s"] * len(ids))
    params = ids + ids
    mapping = {}
    with mysql_conn() as conn:
        with conn.cursor() as c:
            c.execute(
                f"""
                SELECT from_store_id,to_store_id,distance_km,duration_minutes,source
                FROM store_distance_matrix
                WHERE from_store_id IN ({placeholders})
                  AND to_store_id IN ({placeholders})
                """,
                params,
            )
            for row in c.fetchall() or []:
                src = str((row or {}).get("from_store_id") or "").strip()
                dst = str((row or {}).get("to_store_id") or "").strip()
                if not src or not dst:
                    continue
                mapping[(src, dst)] = {
                    "km": (row or {}).get("distance_km"),
                    "min": (row or {}).get("duration_minutes"),
                    "source": str((row or {}).get("source") or "").strip() or "unknown",
                }
    return mapping


def build_dataset():
    rows = load_route_rows()
    resolved = load_store_resolved()
    service_map = load_profile_service()
    capacity_map = load_vehicle_capacity()
    wave1_cfg = load_wave_config_w1()

    # 先整理每车每日 route 集合，识别 X/X+100 双配
    routes_by_day_vehicle = defaultdict(set)
    route_rows = defaultdict(list)
    all_store_ids = set()
    for r in rows:
        d = str((r or {}).get("delivery_date") or "").strip()
        v = str((r or {}).get("vehicle_id") or "").strip()
        rid = _to_int((r or {}).get("route_id"))
        sid = str((r or {}).get("store_id") or "").strip()
        if not d or not v or rid <= 0 or not sid:
            continue
        if _is_sunday(d):
            continue
        routes_by_day_vehicle[(d, v)].add(rid)
        route_rows[(d, v, rid)].append(r)
        all_store_ids.add(sid)

    dist_map = load_distance_map(all_store_ids)

    route_level = []
    stop_level = []

    for (d, v), rid_set in sorted(routes_by_day_vehicle.items()):
        first_route_ids = sorted([rid for rid in rid_set if (rid + 100) in rid_set])
        for rid in first_route_ids:
            key = (d, v, rid)
            stops = sorted(route_rows.get(key, []), key=lambda x: _to_int((x or {}).get("stop_sequence")))
            if not stops:
                continue
            route_name = str((stops[0] or {}).get("route_name") or "").strip()
            store_ids = [str((x or {}).get("store_id") or "").strip() for x in stops if str((x or {}).get("store_id") or "").strip()]
            store_names = [str((x or {}).get("store_name") or "").strip() for x in stops]

            # 距离统计（只算有值的段，缺失单独计数）
            missing_cnt = 0
            source_set = set()
            depot_to_first_km = None
            last_to_depot_km = None
            store_to_store_km_total = 0.0

            if store_ids:
                p = dist_map.get((DC_ID, store_ids[0]))
                if p and p.get("km") is not None:
                    depot_to_first_km = _to_float(p["km"])
                    source_set.add(p.get("source") or "unknown")
                else:
                    missing_cnt += 1

            for i in range(1, len(store_ids)):
                p = dist_map.get((store_ids[i - 1], store_ids[i]))
                if p and p.get("km") is not None:
                    store_to_store_km_total += _to_float(p["km"])
                    source_set.add(p.get("source") or "unknown")
                else:
                    missing_cnt += 1

            if store_ids:
                p = dist_map.get((store_ids[-1], DC_ID))
                if p and p.get("km") is not None:
                    last_to_depot_km = _to_float(p["km"])
                    source_set.add(p.get("source") or "unknown")
                else:
                    missing_cnt += 1

            route_total_km = None
            if depot_to_first_km is not None and last_to_depot_km is not None and missing_cnt == 0:
                route_total_km = round(depot_to_first_km + store_to_store_km_total + last_to_depot_km, 3)

            if not source_set:
                distance_source = "missing"
            elif len(source_set) == 1:
                distance_source = next(iter(source_set))
            else:
                distance_source = "mixed"

            # 装载（原始 + 折算）
            raw_totals = {
                "ambient_turnover_boxes": 0.0,
                "ambient_cartons": 0.0,
                "ambient_full_cases": 0.0,
                "frozen_bags": 0.0,
                "frozen_cartons": 0.0,
                "cold_bins": 0.0,
                "cold_cartons": 0.0,
            }
            resolved_totals = {
                "wave1_load": 0.0,
                "wave2_load": 0.0,
                "wave3_load": 0.0,
                "wave4_load": 0.0,
                "total_resolved_load": 0.0,
            }
            for s in stops:
                raw_totals["ambient_turnover_boxes"] += _to_float((s or {}).get("ambient_turnover_boxes"))
                raw_totals["ambient_cartons"] += _to_float((s or {}).get("ambient_cartons"))
                raw_totals["ambient_full_cases"] += _to_float((s or {}).get("ambient_full_cases"))
                raw_totals["frozen_bags"] += _to_float((s or {}).get("frozen_bags"))
                raw_totals["frozen_cartons"] += _to_float((s or {}).get("frozen_cartons"))
                raw_totals["cold_bins"] += _to_float((s or {}).get("cold_bins"))
                raw_totals["cold_cartons"] += _to_float((s or {}).get("cold_cartons"))

                sid = str((s or {}).get("store_id") or "").strip()
                rr = resolved.get(sid) or {}
                resolved_totals["wave1_load"] += _to_float((rr or {}).get("wave1_load"))
                resolved_totals["wave2_load"] += _to_float((rr or {}).get("wave2_load"))
                resolved_totals["wave3_load"] += _to_float((rr or {}).get("wave3_load"))
                resolved_totals["wave4_load"] += _to_float((rr or {}).get("wave4_load"))
                resolved_totals["total_resolved_load"] += _to_float((rr or {}).get("total_resolved_load"))

            # 波次一线路：total_load_resolved 使用 wave1_load 汇总
            total_load_resolved = round(resolved_totals["wave1_load"], 6)

            capacity = capacity_map.get(v)
            capacity_missing = capacity is None or capacity <= 0
            load_ratio = None
            if not capacity_missing:
                load_ratio = round(total_load_resolved / float(capacity), 6)

            # 到店/离店计算（严格依赖真实时长 + service_minutes）
            route_start_hhmm = str(wave1_cfg.get("start_time") or "").strip()
            if not route_start_hhmm:
                raise RuntimeError("W1.start_time 读取为空，无法计算到店时间。")
            current_dt = _hhmm_to_dt(d, route_start_hhmm)
            prev_store = DC_ID
            last_depart_dt = None
            return_to_depot_minutes = None
            computed_depot_return_time = ""

            for idx, s in enumerate(stops, start=1):
                sid = str((s or {}).get("store_id") or "").strip()
                sname = str((s or {}).get("store_name") or "").strip()
                pair = dist_map.get((prev_store, sid))

                seg_min = None
                seg_km = None
                if pair and pair.get("min") is not None:
                    seg_min = _to_float(pair["min"])
                if pair and pair.get("km") is not None:
                    seg_km = _to_float(pair["km"])

                svc = service_map.get(sid)
                if svc in (None, "", 0):
                    svc = 15
                svc_missing = False
                arrival_dt = None
                depart_dt = None

                # 不推测：分段时长或服务时长缺失则该点时间不可计算
                if seg_min is not None and not svc_missing and current_dt is not None:
                    arrival_dt = current_dt + timedelta(minutes=seg_min)
                    depart_dt = arrival_dt + timedelta(minutes=int(svc))
                    current_dt = depart_dt
                else:
                    current_dt = None

                stop_level.append(
                    {
                        "delivery_date": d,
                        "vehicle_id": v,
                        "route_id": rid,
                        "wave_tag": "W1",
                        "route_start_time": route_start_hhmm,
                        "route_name": route_name,
                        "stop_sequence": _to_int((s or {}).get("stop_sequence")),
                        "store_id": sid,
                        "store_name": sname,
                        "segment_minutes_from_prev": "" if seg_min is None else round(seg_min, 3),
                        "segment_km_from_prev": "" if seg_km is None else round(seg_km, 3),
                        "computed_arrival_time": _fmt_dt(arrival_dt),
                        "service_minutes": "" if svc_missing else int(svc),
                        "computed_departure_time": _fmt_dt(depart_dt),
                    }
                )
                prev_store = sid
                if depart_dt is not None:
                    last_depart_dt = depart_dt

            back = dist_map.get((store_ids[-1], DC_ID)) if store_ids else None
            back_min = _to_float(back.get("min")) if back and back.get("min") is not None else None
            if last_depart_dt is not None and back_min is not None:
                return_to_depot_minutes = round(back_min, 3)
                computed_depot_return_time = _fmt_dt(last_depart_dt + timedelta(minutes=back_min))

            route_level.append(
                {
                    "delivery_date": d,
                    "vehicle_id": v,
                    "route_id": rid,
                    "wave_tag": "W1",
                    "route_start_time": route_start_hhmm,
                    "route_start_source": str(wave1_cfg.get("source") or ""),
                    "cutoff_rule": str(wave1_cfg.get("cutoff_rule") or ""),
                    "wave_end_time": str(wave1_cfg.get("end_time") or ""),
                    "route_name": route_name,
                    "stop_count": len(store_ids),
                    "store_ids": ",".join(store_ids),
                    "store_names": ",".join(store_names),
                    "depot_to_first_km": "" if depot_to_first_km is None else round(depot_to_first_km, 3),
                    "store_to_store_km_total": round(store_to_store_km_total, 3),
                    "last_to_depot_km": "" if last_to_depot_km is None else round(last_to_depot_km, 3),
                    "route_total_km": "" if route_total_km is None else route_total_km,
                    "distance_missing_segment_count": int(missing_cnt),
                    "distance_source": distance_source,
                    "raw_ambient_turnover_boxes": round(raw_totals["ambient_turnover_boxes"], 6),
                    "raw_ambient_cartons": round(raw_totals["ambient_cartons"], 6),
                    "raw_ambient_full_cases": round(raw_totals["ambient_full_cases"], 6),
                    "raw_frozen_bags": round(raw_totals["frozen_bags"], 6),
                    "raw_frozen_cartons": round(raw_totals["frozen_cartons"], 6),
                    "raw_cold_bins": round(raw_totals["cold_bins"], 6),
                    "raw_cold_cartons": round(raw_totals["cold_cartons"], 6),
                    "resolved_wave1_load_sum": round(resolved_totals["wave1_load"], 6),
                    "resolved_wave2_load_sum": round(resolved_totals["wave2_load"], 6),
                    "resolved_wave3_load_sum": round(resolved_totals["wave3_load"], 6),
                    "resolved_wave4_load_sum": round(resolved_totals["wave4_load"], 6),
                    "resolved_total_load_sum": round(resolved_totals["total_resolved_load"], 6),
                    "total_load_resolved": total_load_resolved,
                    "load_ratio": "" if load_ratio is None else load_ratio,
                    "capacity_missing": "true" if capacity_missing else "false",
                    "capacity_missing_note": "" if not capacity_missing else "wms_vehicle_snapshot.payload_json未提供容量字段",
                    "last_store_departure_time": _fmt_dt(last_depart_dt),
                    "return_to_depot_minutes": "" if return_to_depot_minutes is None else return_to_depot_minutes,
                    "computed_depot_return_time": computed_depot_return_time,
                }
            )

    route_level.sort(key=lambda x: (x["delivery_date"], x["vehicle_id"], _to_int(x["route_id"])))
    stop_level.sort(
        key=lambda x: (x["delivery_date"], x["vehicle_id"], _to_int(x["route_id"]), _to_int(x["stop_sequence"]))
    )
    return route_level, stop_level, wave1_cfg


def write_csv(path: Path, rows, fieldnames):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in rows:
            writer.writerow(r)


def main():
    route_level, stop_level, wave1_cfg = build_dataset()

    route_fields = [
        "delivery_date",
        "vehicle_id",
        "route_id",
        "wave_tag",
        "route_start_time",
        "route_start_source",
        "cutoff_rule",
        "wave_end_time",
        "route_name",
        "stop_count",
        "store_ids",
        "store_names",
        "depot_to_first_km",
        "store_to_store_km_total",
        "last_to_depot_km",
        "route_total_km",
        "distance_missing_segment_count",
        "distance_source",
        "raw_ambient_turnover_boxes",
        "raw_ambient_cartons",
        "raw_ambient_full_cases",
        "raw_frozen_bags",
        "raw_frozen_cartons",
        "raw_cold_bins",
        "raw_cold_cartons",
        "resolved_wave1_load_sum",
        "resolved_wave2_load_sum",
        "resolved_wave3_load_sum",
        "resolved_wave4_load_sum",
        "resolved_total_load_sum",
        "total_load_resolved",
        "load_ratio",
        "capacity_missing",
        "capacity_missing_note",
        "last_store_departure_time",
        "return_to_depot_minutes",
        "computed_depot_return_time",
    ]
    stop_fields = [
        "delivery_date",
        "vehicle_id",
        "route_id",
        "wave_tag",
        "route_start_time",
        "route_name",
        "stop_sequence",
        "store_id",
        "store_name",
        "segment_minutes_from_prev",
        "segment_km_from_prev",
        "computed_arrival_time",
        "service_minutes",
        "computed_departure_time",
    ]

    write_csv(ROUTE_CSV, route_level, route_fields)
    write_csv(STOP_CSV, stop_level, stop_fields)
    print(f"route_level_rows={len(route_level)}")
    print(f"stop_level_rows={len(stop_level)}")
    print(f"wave_config_source={wave1_cfg.get('source')}")
    print(f"W1.start_time={wave1_cfg.get('start_time')}")
    print(f"W1.end_time={wave1_cfg.get('end_time')}")
    print(f"W1.cutoff_rule={wave1_cfg.get('cutoff_rule')}")
    print(str(ROUTE_CSV))
    print(str(STOP_CSV))


if __name__ == "__main__":
    main()
