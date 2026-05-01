"""
车辆驱动构造算法：以车辆为中心逐台装店，强调先得到可行解再补排。
"""
# vehicle_driven.py
# 车辆驱动构造算法 - 按车构建，支持车辆隔离和里程限制
# W1/W2接力：同一辆车两趟往返总里程≤240km
# W3独立：单程≤260km，车速48km/h，且不使用前序波次用过的车辆
# W2继承W1路线：同一辆车优先跑W1中跑过的门店（若属于W2），按W2时间窗重新排序
# 数据源：store_wave_load_resolved + C_SHOP_MAIN（字段必须存在，否则跳过门店）
# 距离：store_distance_matrix（真实路网，缺失则不可行）
# 输出TXT明细（含店名）并写入六张调度结果表

import os
import json
import random
import time
from datetime import datetime
import pymysql

# ========== 日志配置 ==========
LOG_FILE = r"C:\Users\laoj0\Desktop\123.txt"
DEBUG_LOG = True

def _log(msg):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    full_msg = f"[{timestamp}] {msg}"
    if DEBUG_LOG:
        print(full_msg, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(full_msg + "\n")
    except Exception:
        pass

# ========== 数据库连接 ==========
try:
    from config import settings
    MYSQL_HOST = settings.MYSQL_HOST
    MYSQL_PORT = settings.MYSQL_PORT
    MYSQL_USER = settings.MYSQL_USER
    MYSQL_PASSWORD = settings.MYSQL_PASSWORD
    MYSQL_DATABASE = settings.MYSQL_DATABASE
except Exception:
    MYSQL_HOST = "127.0.0.1"
    MYSQL_PORT = 3306
    MYSQL_USER = "root"
    MYSQL_PASSWORD = ""
    MYSQL_DATABASE = "whale_scheduler"

def _get_db_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

# ========== 距离缓存 ==========
_distance_cache = {}

def get_distance(from_id, to_id, dist_payload):
    if from_id == to_id:
        return 0.0
    d = dist_payload.get(from_id, {}).get(to_id, 0.0)
    if d > 0:
        return d
    cache_key = f"{from_id}|{to_id}"
    if cache_key in _distance_cache:
        return _distance_cache[cache_key]
    try:
        with _get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT distance_km FROM store_distance_matrix WHERE from_store_id=%s AND to_store_id=%s",
                    (from_id, to_id)
                )
                row = cursor.fetchone()
                if row and row.get("distance_km", 0) > 0:
                    d = float(row["distance_km"])
                    _distance_cache[cache_key] = d
                    return d
    except Exception as e:
        _log(f"距离查询失败: {e}")
    return None

# ========== 辅助函数 ==========
def time_str_to_minutes(t_str, start_min):
    if not t_str or ":" not in t_str:
        return start_min
    h, m = map(int, t_str.split(":"))
    val = h * 60 + m
    if val < start_min:
        val += 1440
    return val

def minutes_to_time_str(min_val):
    day = 24 * 60
    norm = ((min_val % day) + day) % day
    h = int(norm // 60)
    m = int(norm % 60)
    return f"{h:02d}:{m:02d}"

# ========== 硬约束参数 ==========
MAX_CAPACITY = 1.2
MAX_STOPS_PER_TRIP_W1W2 = 12
MAX_MILEAGE_W1W2_TOTAL = 240.0      # W1+W2 往返总里程上限
MAX_MILEAGE_W3_ONE_WAY = 260.0      # W3 单程上限
SPEED_W3 = 48.0                     # W3 车速 km/h
SPEED_DEFAULT = 38.0                # 其他波次车速

# ========== 从数据库加载门店（所有字段必须存在，否则跳过） ==========
def load_stores_from_db(wave):
    wave_id = wave.get("waveId", "").upper()
    if wave_id == "W1":
        load_field = "wave1_load"
        time_field = "first_wave_time"
    elif wave_id == "W2":
        load_field = "wave2_load"
        time_field = "second_wave_time"
    elif wave_id == "W3":
        load_field = "wave3_load"
        time_field = "arrival_time_w3"
    elif wave_id == "W4":
        load_field = "wave4_load"
        time_field = "arrival_time_w4"
    else:
        raise ValueError(f"unsupported_wave_id:{wave_id}")

    wave_no = wave_id[1]  # "1","2","3","4"

    with _get_db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT shop_code, wave_belongs, {load_field} AS boxes, {time_field} AS desired_time
                FROM store_wave_load_resolved
                WHERE FIND_IN_SET(%s, wave_belongs) > 0
                """,
                (wave_no,)
            )
            resolved_rows = cursor.fetchall() or []
            if not resolved_rows:
                _log(f"store_wave_load_resolved 中无波次 {wave_id} 的门店")
                return []

            shop_codes = [row["shop_code"] for row in resolved_rows]
            if shop_codes:
                placeholders = ",".join(["%s"] * len(shop_codes))
                cursor.execute(
                    f"""
                    SELECT shop_code, shop_name, service_minutes, allowed_late_minutes, difficulty
                    FROM C_SHOP_MAIN
                    WHERE shop_code IN ({placeholders})
                    """,
                    shop_codes
                )
                shop_rows = cursor.fetchall() or []
                shop_map = {row["shop_code"]: row for row in shop_rows}
            else:
                shop_map = {}

    stores = {}
    for row in resolved_rows:
        code = row["shop_code"]
        boxes = float(row.get("boxes", 0.0))
        if boxes <= 0:
            _log(f"门店 {code} 货量为0，已过滤")
            continue
        desired_time_str = row.get("desired_time") or ""
        desired_min = time_str_to_minutes(desired_time_str, wave["startMin"])
        shop_info = shop_map.get(code)
        if not shop_info:
            _log(f"门店 {code} 在 C_SHOP_MAIN 中无记录，已过滤")
            continue

        store_name = shop_info.get("shop_name", code)
        service_min = shop_info.get("service_minutes")
        if service_min is None:
            _log(f"门店 {code} 的 service_minutes 为空，已过滤")
            continue
        service_min = int(service_min)

        allowed_late = shop_info.get("allowed_late_minutes")
        if allowed_late is None:
            _log(f"门店 {code} 的 allowed_late_minutes 为空，已过滤")
            continue
        allowed_late = int(allowed_late)

        difficulty = shop_info.get("difficulty")
        if difficulty is None:
            _log(f"门店 {code} 的 difficulty 为空，已过滤")
            continue
        difficulty = float(difficulty)

        actual_service = service_min * max(0.1, difficulty)

        stores[code] = {
            "id": code,
            "name": store_name,
            "boxes": boxes,
            "desiredArrivalMin": desired_min,
            "latestAllowedArrivalMin": desired_min + allowed_late,
            "serviceMinutes": service_min,
            "actualServiceMinutes": actual_service,
            "difficulty": difficulty,
            "parking": allowed_late,
            "waveBelongs": row.get("wave_belongs", ""),
        }
    _log(f"加载门店 {len(stores)} 家（波次 {wave_id}）")
    return stores

# ========== 路线可行性检查（硬约束） ==========
def is_route_feasible(vehicle, route, stores, dist_payload, wave, scenario, prior_stats=None):
    wave_id = wave.get("waveId", "").upper()
    if wave_id == "W3":
        speed = SPEED_W3
    else:
        speed = SPEED_DEFAULT

    if wave_id in ("W3", "W4"):
        start_min = max(
            wave["startMin"],
            vehicle.get("earliestDepartureMin", wave["startMin"]),
            scenario.get("dispatchStartMin", wave["startMin"]),
        )
    else:
        start_min = max(
            wave["startMin"],
            vehicle.get("earliestDepartureMin", wave["startMin"]),
            scenario.get("dispatchStartMin", wave["startMin"]),
        )
        if prior_stats and "finish_time" in prior_stats:
            start_min = max(start_min, prior_stats["finish_time"])

    current_time = start_min
    current_node = "DC"
    cumulative_load = 0.0
    one_way_distance = 0.0
    round_distance = 0.0

    for store_id in route:
        store = stores.get(store_id)
        if not store:
            return False, f"门店{store_id}不存在"
        boxes = store["boxes"]
        cumulative_load += boxes
        if cumulative_load > MAX_CAPACITY:
            return False, f"容量超限 {cumulative_load:.3f} > {MAX_CAPACITY}"

        leg_distance = get_distance(current_node, store_id, dist_payload)
        if leg_distance is None:
            return False, f"距离缺失 {current_node}->{store_id}"
        travel_min = leg_distance / max(speed, 1.0) * 60.0
        arrival = current_time + travel_min
        desired = store["desiredArrivalMin"]
        latest_allowed = store["latestAllowedArrivalMin"]
        if desired < start_min:
            desired += 1440
            latest_allowed += 1440
        if arrival > latest_allowed:
            return False, f"时间窗违反 {minutes_to_time_str(arrival)} > {minutes_to_time_str(latest_allowed)}"

        current_time = arrival + store["actualServiceMinutes"]
        one_way_distance += leg_distance
        current_node = store_id

    back_distance = get_distance(current_node, "DC", dist_payload)
    if back_distance is None:
        return False, f"距离缺失 {current_node}->DC"
    round_distance = one_way_distance + back_distance

    if wave_id in ("W1", "W2"):
        if len(route) > MAX_STOPS_PER_TRIP_W1W2:
            return False, f"门店数超限 {len(route)} > {MAX_STOPS_PER_TRIP_W1W2}"
        prior_round = prior_stats.get("prior_round_distance", 0.0) if prior_stats else 0.0
        if prior_round + round_distance > MAX_MILEAGE_W1W2_TOTAL:
            return False, f"里程超限 {prior_round+round_distance:.1f} > {MAX_MILEAGE_W1W2_TOTAL}"
    elif wave_id == "W3":
        if one_way_distance > MAX_MILEAGE_W3_ONE_WAY:
            return False, f"W3单程超限 {one_way_distance:.1f} > {MAX_MILEAGE_W3_ONE_WAY}"
    # W4 无里程限制

    return True, ""

# ========== 计算路线代价（用于选择最优插入） ==========
def compute_route_cost(vehicle, route, stores, dist_payload, wave, scenario, prior_stats=None):
    wave_id = wave.get("waveId", "").upper()
    if wave_id == "W3":
        speed = SPEED_W3
    else:
        speed = SPEED_DEFAULT

    if wave_id in ("W3", "W4"):
        start_min = max(
            wave["startMin"],
            vehicle.get("earliestDepartureMin", wave["startMin"]),
            scenario.get("dispatchStartMin", wave["startMin"]),
        )
    else:
        start_min = max(
            wave["startMin"],
            vehicle.get("earliestDepartureMin", wave["startMin"]),
            scenario.get("dispatchStartMin", wave["startMin"]),
        )
        if prior_stats and "finish_time" in prior_stats:
            start_min = max(start_min, prior_stats["finish_time"])

    current_time = start_min
    current_node = "DC"
    total_distance = 0.0
    total_lateness = 0.0
    for store_id in route:
        store = stores.get(store_id)
        leg_distance = get_distance(current_node, store_id, dist_payload)
        if leg_distance is None:
            return 1e9
        travel_min = leg_distance / max(speed, 1.0) * 60.0
        arrival = current_time + travel_min
        desired = store["desiredArrivalMin"]
        if desired < start_min:
            desired += 1440
        lateness = max(0, arrival - desired)
        total_lateness += lateness
        current_time = arrival + store["actualServiceMinutes"]
        total_distance += leg_distance
        current_node = store_id
    back_distance = get_distance(current_node, "DC", dist_payload)
    if back_distance is None:
        return 1e9
    total_distance += back_distance
    return total_distance + total_lateness * 0.5

# ========== 按车构建：为当前车辆选择最佳门店插入 ==========
def best_insert_for_vehicle(vehicle, current_route, pending_ids, stores, dist_payload, wave, scenario, prior_stats):
    best_store = None
    best_pos = -1
    best_delta = float('inf')
    base_routes = [current_route] if current_route else []
    base_cost = compute_route_cost(vehicle, current_route, stores, dist_payload, wave, scenario, prior_stats)

    for store_id in pending_ids:
        for pos in range(len(current_route) + 1):
            new_route = current_route[:pos] + [store_id] + current_route[pos:]
            feasible, _ = is_route_feasible(vehicle, new_route, stores, dist_payload, wave, scenario, prior_stats)
            if not feasible:
                continue
            new_cost = compute_route_cost(vehicle, new_route, stores, dist_payload, wave, scenario, prior_stats)
            delta = new_cost - base_cost
            if delta < best_delta:
                best_delta = delta
                best_store = store_id
                best_pos = pos
    if best_store is None:
        return None, None, None
    new_route = current_route[:best_pos] + [best_store] + current_route[best_pos:]
    return best_store, new_route, best_delta

# ========== 计算路线详细信息（用于输出和数据库） ==========
def compute_route_with_times(vehicle, route, stores, dist_payload, wave, scenario, prior_stats=None):
    wave_id = wave.get("waveId", "").upper()
    if wave_id == "W3":
        speed = SPEED_W3
    else:
        speed = SPEED_DEFAULT

    if wave_id in ("W3", "W4"):
        start_min = max(
            wave["startMin"],
            vehicle.get("earliestDepartureMin", wave["startMin"]),
            scenario.get("dispatchStartMin", wave["startMin"]),
        )
    else:
        start_min = max(
            wave["startMin"],
            vehicle.get("earliestDepartureMin", wave["startMin"]),
            scenario.get("dispatchStartMin", wave["startMin"]),
        )
        if prior_stats and "finish_time" in prior_stats:
            start_min = max(start_min, prior_stats["finish_time"])

    current_time = start_min
    total_distance = 0.0
    total_load = 0.0
    cumulative_load = 0.0
    stops_info = []
    current_node = "DC"
    one_way_distance = 0.0

    for store_id in route:
        store = stores.get(store_id)
        if not store:
            return 0, 0, [], False, f"门店{store_id}不存在"
        boxes = store["boxes"]
        cumulative_load += boxes
        if cumulative_load > MAX_CAPACITY:
            return 0, 0, [], False, f"容量超限 {cumulative_load:.3f} > {MAX_CAPACITY}"

        leg_distance = get_distance(current_node, store_id, dist_payload)
        if leg_distance is None:
            return 0, 0, [], False, f"距离缺失 {current_node}->{store_id}"
        travel_min = leg_distance / max(speed, 1.0) * 60.0
        arrival = current_time + travel_min
        desired = store["desiredArrivalMin"]
        latest_allowed = store["latestAllowedArrivalMin"]
        if desired < start_min:
            desired += 1440
            latest_allowed += 1440
        if arrival > latest_allowed:
            return 0, 0, [], False, f"时间窗违反 {minutes_to_time_str(arrival)} > {minutes_to_time_str(latest_allowed)}"

        leave = arrival + store["actualServiceMinutes"]
        stops_info.append({
            "store_id": store_id,
            "store_name": store["name"],
            "boxes": boxes,
            "cumulative_load": cumulative_load,
            "desired_arrival_min": desired,
            "arrival_min": arrival,
            "leave_min": leave,
            "expected_time_str": minutes_to_time_str(desired),
            "arrival_time_str": minutes_to_time_str(arrival),
            "leave_time_str": minutes_to_time_str(leave),
            "leg_distance_km": leg_distance,
            "cumulative_distance_km": total_distance + leg_distance,
        })
        total_load += boxes
        total_distance += leg_distance
        one_way_distance += leg_distance
        current_node = store_id
        current_time = leave

    back_distance = get_distance(current_node, "DC", dist_payload)
    if back_distance is None:
        return 0, 0, [], False, f"距离缺失 {current_node}->DC"
    total_distance += back_distance

    if wave_id in ("W1", "W2"):
        prior_round = prior_stats.get("prior_round_distance", 0.0) if prior_stats else 0.0
        if prior_round + total_distance > MAX_MILEAGE_W1W2_TOTAL:
            return 0, 0, [], False, f"总里程超限 {prior_round+total_distance:.1f} > {MAX_MILEAGE_W1W2_TOTAL}"
        if len(route) > MAX_STOPS_PER_TRIP_W1W2:
            return 0, 0, [], False, f"门店数超限 {len(route)} > {MAX_STOPS_PER_TRIP_W1W2}"
    elif wave_id == "W3":
        if one_way_distance > MAX_MILEAGE_W3_ONE_WAY:
            return 0, 0, [], False, f"W3单程超限 {one_way_distance:.1f} > {MAX_MILEAGE_W3_ONE_WAY}"

    return total_distance, total_load, stops_info, True, ""

# ========== 输出明细文件和写入数据库 ==========
def output_and_save(wave_id, state, stores, dist_payload, wave, scenario, prior_stats_by_plate, batch_id, decision_log):
    # 1. 写TXT文件
    filename = f"C:\\Users\\laoj0\\Desktop\\dispatch_detail_{wave_id}.txt"
    try:
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"========== 波次 {wave_id} 调度明细 ==========\n\n")
            for vehicle in state:
                plate = vehicle["plateNo"]
                routes = vehicle["routes"]
                if not routes:
                    continue
                prior_stats = prior_stats_by_plate.get(plate, {})
                for ri, route in enumerate(routes):
                    total_dist, total_load, stops_info, feasible, reason = compute_route_with_times(
                        vehicle, route, stores, dist_payload, wave, scenario, prior_stats
                    )
                    if not feasible:
                        f.write(f"车辆 {plate} 路线{ri+1} 不可行: {reason}\n\n")
                        continue
                    f.write(f"车辆 {plate} | 路线{ri+1} | 门店数:{len(route)} | 总里程: {total_dist:.1f} km | 总装载: {total_load:.3f}\n")
                    f.write("\n序号\t店铺编号\t店铺名称\t货量\t累计占用\t期望到店\t计划到店\t计划离店\n")
                    for idx, stop in enumerate(stops_info, 1):
                        f.write(f"{idx}\t{stop['store_id']}\t{stop['store_name']}\t{stop['boxes']:.3f}\t{stop['cumulative_load']:.3f}\t"
                                f"{stop['expected_time_str']}\t{stop['arrival_time_str']}\t{stop['leave_time_str']}\n")
                    f.write("\n")
            f.write(f"注：容量上限 {MAX_CAPACITY}，允许晚到（从C_SHOP_MAIN读取），W1/W2往返总里程≤{MAX_MILEAGE_W1W2_TOTAL}km，每趟≤{MAX_STOPS_PER_TRIP_W1W2}店；W3单程≤{MAX_MILEAGE_W3_ONE_WAY}km，车速{SPEED_W3}km/h。\n")
        _log(f"详细调度结果已写入 {filename}")
    except Exception as e:
        _log(f"写入TXT失败: {e}")

    # 2. 写入数据库（六张表）
    try:
        conn = _get_db_connection()
        cursor = conn.cursor()

        total_stores = len(stores)
        scheduled_set = set()
        for v in state:
            for route in v["routes"]:
                for sid in route:
                    scheduled_set.add(sid)
        scheduled_count = len(scheduled_set)
        unscheduled_count = total_stores - scheduled_count

        total_wave_distance = 0.0
        total_wave_load = 0.0
        for v in state:
            plate = v["plateNo"]
            prior = prior_stats_by_plate.get(plate, {})
            for route in v["routes"]:
                total_dist, total_load, _, feasible, _ = compute_route_with_times(
                    v, route, stores, dist_payload, wave, scenario, prior
                )
                if feasible:
                    total_wave_distance += total_dist
                    total_wave_load += total_load

        # dispatch_wave_result
        cursor.execute(
            """INSERT INTO dispatch_wave_result
               (batch_id, wave_id, stores_count, scheduled_count, unscheduled_count,
                total_distance_km, total_load, total_cost, start_min, end_min, end_mode)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (batch_id, wave_id, total_stores, scheduled_count, unscheduled_count,
             total_wave_distance, total_wave_load, 0.0, wave["startMin"], wave["endMin"], wave.get("endMode", "return"))
        )

        # dispatch_route_detail 和 dispatch_vehicle_snapshot
        for v in state:
            plate = v["plateNo"]
            prior = prior_stats_by_plate.get(plate, {})
            routes = v["routes"]
            vehicle_total_dist = 0.0
            vehicle_total_load = 0.0
            trip_count = len(routes)
            for trip_idx, route in enumerate(routes, 1):
                total_dist, total_load, stops_info, feasible, _ = compute_route_with_times(
                    v, route, stores, dist_payload, wave, scenario, prior
                )
                if not feasible:
                    continue
                vehicle_total_dist += total_dist
                vehicle_total_load += total_load
                cumulative_km = 0.0
                for stop_idx, stop in enumerate(stops_info, 1):
                    leg_km = stop["leg_distance_km"]
                    cumulative_km += leg_km
                    arrival_lateness = max(0, stop["arrival_min"] - stop["desired_arrival_min"])
                    cursor.execute(
                        """INSERT INTO dispatch_route_detail
                           (batch_id, wave_id, vehicle_plate, trip_no, stop_seq, store_id,
                            boxes, cumulative_load, desired_arrival_time, planned_arrival_time, planned_leave_time,
                            leg_distance_km, cumulative_distance_km, arrival_lateness_min)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                        (batch_id, wave_id, plate, trip_idx, stop_idx, stop["store_id"],
                         stop["boxes"], stop["cumulative_load"], stop["expected_time_str"],
                         stop["arrival_time_str"], stop["leave_time_str"],
                         leg_km, cumulative_km, arrival_lateness)
                    )
            # dispatch_vehicle_snapshot
            routes_json = json.dumps(routes, ensure_ascii=False)
            cursor.execute(
                """INSERT INTO dispatch_vehicle_snapshot
                   (batch_id, wave_id, vehicle_plate, routes_json, total_distance_km, total_load, trip_count)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, wave_id, plate, routes_json, vehicle_total_dist, vehicle_total_load, trip_count)
            )

        # dispatch_unscheduled_store
        unscheduled_ids = [sid for sid in stores.keys() if sid not in scheduled_set]
        for sid in unscheduled_ids:
            store = stores[sid]
            cursor.execute(
                """INSERT INTO dispatch_unscheduled_store
                   (batch_id, wave_id, store_id, desired_arrival_time, boxes, reason, reason_detail)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, wave_id, sid, minutes_to_time_str(store["desiredArrivalMin"]),
                 store["boxes"], "time_window_or_capacity", "未能找到可行插入位置")
            )

        # dispatch_batch
        cursor.execute(
            """INSERT INTO dispatch_batch
               (batch_id, strategy, wave_ids, total_stores, total_vehicles,
                total_cost, total_distance_km, total_load, unscheduled_count, duration_ms)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (batch_id, "vehicle_driven", wave_id, total_stores, len(state),
             0.0, total_wave_distance, total_wave_load, unscheduled_count, 0)
        )

        # dispatch_algorithm_log
        for log_entry in decision_log:
            cursor.execute(
                """INSERT INTO dispatch_algorithm_log
                   (batch_id, wave_id, step_no, vehicle_plate, store_id, cost_delta, remaining_stores, candidates_json)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, wave_id, log_entry["step"], log_entry["vehicle_plate"],
                 log_entry["store_id"], log_entry["cost_delta"], log_entry["remaining"],
                 json.dumps(log_entry.get("candidates", []), ensure_ascii=False))
            )

        conn.commit()
        cursor.close()
        conn.close()
        _log(f"数据库写入完成，batch_id={batch_id}")
    except Exception as e:
        _log(f"数据库写入失败（不影响调度结果）: {e}")

# ========== 主求解函数 ==========
def solve_vehicle_driven(payload):
    start_time = time.time()
    scenario = payload["scenario"]
    wave = payload["wave"]
    dist_payload = payload.get("dist", {})
    wave_id = wave.get("waveId", "").upper()
    excluded_vehicles = payload.get("excluded_vehicles", [])
    w1_assignments = payload.get("w1_assignments", {})   # 仅 W2 会传入

    _log(f"\n========== 开始波次 {wave_id} 车辆驱动构造 ==========")
    _log(f"日志文件位置: {LOG_FILE}")
    if excluded_vehicles:
        _log(f"排除车辆: {', '.join(excluded_vehicles)}")
    if w1_assignments:
        _log(f"收到 W1 门店分配: {len(w1_assignments)} 辆车")

    batch_id = f"vehicle_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{wave_id}"

    stores = load_stores_from_db(wave)
    if not stores:
        _log("没有加载到任何门店，返回空解")
        return {"bestState": [], "traceLog": []}
    _log(f"门店总数: {len(stores)}")

    # 获取车辆列表（前端已过滤或排序）
    base_vehicles = payload.get("vehicles", [])
    if excluded_vehicles:
        excluded_set = set(excluded_vehicles)
        base_vehicles = [v for v in base_vehicles if v.get("plateNo") not in excluded_set]
        _log(f"排除后剩余车辆数: {len(base_vehicles)}")

    # 注意：不再随机打乱车辆顺序，保持前端传入的顺序（W1顺序已由前端保持）
    # 对于 W2，前端传入的 vehicles 顺序与 W1 一致，且 w1_assignments 提供了 W1 的门店列表

    state = []
    for v in base_vehicles:
        state.append({
            "plateNo": v.get("plateNo"),
            "capacity": v.get("capacity", 1),
            "speed": v.get("speed", 38.0),
            "earliestDepartureMin": v.get("earliestDepartureMin", 0),
            "priorRegularDistance": v.get("priorRegularDistance", 0.0),
            "priorWaveCount": v.get("priorWaveCount", 0),
            "routes": [],
        })
    _log(f"车辆数: {len(state)}")

    # 前序接力统计（W1/W2需要，W3/W4忽略）
    prior_stats_by_plate = {}
    for v in base_vehicles:
        plate = v.get("plateNo")
        if plate:
            prior_stats_by_plate[plate] = {
                "finish_time": v.get("earliestDepartureMin", 0),
                "prior_round_distance": v.get("prior_round_distance", 0.0),
                "priorWaveCount": v.get("priorWaveCount", 0),
            }

    pending = set(stores.keys())
    decision_log = []
    step = 0

    # 对于 W2，如果有 w1_assignments，先为每辆车构建初始路线（继承 W1 中属于 W2 的门店，并按期望时间排序）
    if wave_id == "W2" and w1_assignments:
        _log("W2 继承 W1 路线：优先保留同一车辆在 W1 中跑过的门店（若属于 W2）")
        # 预先计算每个门店是否属于 W2（已在 stores 中，因为只加载了 W2 门店）
        for v_idx, vehicle in enumerate(state):
            plate = vehicle["plateNo"]
            w1_stores = w1_assignments.get(plate, [])
            if not w1_stores:
                continue
            # 过滤出属于 W2 的门店（即存在于 pending 中的门店）
            inherited = [sid for sid in w1_stores if sid in pending]
            if not inherited:
                continue
            # 按 W2 的期望时间重新排序
            inherited.sort(key=lambda sid: stores[sid]["desiredArrivalMin"])
            # 检查该路线是否可行（容量、时间窗等）
            feasible, _ = is_route_feasible(vehicle, inherited, stores, dist_payload, wave, scenario, prior_stats_by_plate.get(plate, {}))
            if feasible:
                # 将继承的路线设置为当前车辆的初始路线，并从 pending 中移除这些门店
                state[v_idx]["routes"] = [inherited]
                for sid in inherited:
                    pending.remove(sid)
                _log(f"车辆 {plate} 继承 W1 路线 {len(inherited)} 店: {inherited}")
                # 记录决策日志（可选）
                step += 1
                decision_log.append({
                    "step": step,
                    "vehicle_plate": plate,
                    "store_id": "INHERIT",
                    "cost_delta": 0.0,
                    "remaining": len(pending),
                    "candidates": []
                })
            else:
                _log(f"车辆 {plate} 继承 W1 路线不可行（容量/时间窗/里程），将重新分配")

    # 按车构建（每辆车一趟），在已有初始路线基础上继续插入剩余门店
    for v_idx, vehicle in enumerate(state):
        plate = vehicle["plateNo"]
        prior = prior_stats_by_plate.get(plate, {})
        current_route = vehicle["routes"][0] if vehicle["routes"] else []
        _log(f"\n开始填充车辆 {plate}，当前待分配门店数: {len(pending)}，已有路线门店数: {len(current_route)}")
        while pending:
            best_store, new_route, delta = best_insert_for_vehicle(
                vehicle, current_route, pending, stores, dist_payload, wave, scenario, prior
            )
            if best_store is None:
                break
            step += 1
            current_route = new_route
            pending.remove(best_store)
            decision_log.append({
                "step": step,
                "vehicle_plate": plate,
                "store_id": best_store,
                "cost_delta": delta,
                "remaining": len(pending),
                "candidates": []
            })
            _log(f"  第 {step} 轮：分配门店 {best_store} 到车辆 {plate}，代价增量 {delta:.2f}，剩余 {len(pending)} 家")
        if current_route:
            state[v_idx]["routes"] = [current_route]
            _log(f"车辆 {plate} 填充结束，路线门店数 {len(current_route)}，剩余待分配: {len(pending)}")
        else:
            _log(f"车辆 {plate} 未分配任何门店")

    # 输出并保存
    output_and_save(wave_id, state, stores, dist_payload, wave, scenario, prior_stats_by_plate, batch_id, decision_log)

    _log(f"波次 {wave_id} 求解完成，已调度 {len(stores)-len(pending)} 家，未调度 {len(pending)} 家")
    return {"bestState": state, "traceLog": []}