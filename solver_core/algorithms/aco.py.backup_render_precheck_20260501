"""
蚁群算法（ACO）求解器。

这份实现服务于当前调度系统的三个场景：
1. 主调度页面按波次求解。
2. 单店时间推演做 A/B 对照。
3. 混沌页按“当前剩余集”继续求解。

实现目标不是写成教科书式的纯学术版本，而是：
- 严格复用项目里的真实车辆、门店、距离矩阵、时间窗约束。
- 输出可回放的逐店到店时间，方便前端展示和推演审计。
- 在 W2 继承 W1、W3/W4 独立求解这些业务规则上保持兼容。
"""

import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

import pymysql

# 调试日志默认写到桌面文件，方便业务侧在后台直接核对求解过程。
LOG_FILE = r"C:\Users\laoj0\Desktop\123.txt"
DEBUG_LOG = True


def _log(message: str) -> None:
    """统一日志出口。

    ACO 求解比较依赖过程观察，这里同时打控制台和桌面日志文件。
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    full_message = f"[ACO] [{timestamp}] {message}"
    if DEBUG_LOG:
        print(full_message, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as handler:
            handler.write(full_message + "\n")
    except Exception:
        pass


# 数据库配置优先读项目配置，读不到时退回本地默认值。
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
    """获取 MySQL 连接。"""
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


# 这些约束值是当前业务页面和算法共识口径，不能在展示层私自另算。
MAX_CAPACITY = 1.2
MAX_STOPS_PER_TRIP_W1W2 = 12
MAX_MILEAGE_W1W2_TOTAL = 240.0
MAX_MILEAGE_W3_ONE_WAY = 260.0
SPEED_W3 = 48.0
SPEED_DEFAULT = 38.0

# 距离查询做进程内缓存，避免蚁群迭代里频繁打数据库。
_DISTANCE_CACHE: Dict[Tuple[str, str], Tuple[float, float]] = {}


def time_str_to_minutes(time_text: str, start_min: float) -> float:
    """把 HH:MM 转成分钟值。

    如果门店时间落在波次开始之前，视为跨天时间并加一天。
    """
    if not time_text or ":" not in time_text:
        return float(start_min)
    hour, minute = map(int, str(time_text).split(":"))
    value = hour * 60 + minute
    if value < start_min:
        value += 1440
    return float(value)


def minutes_to_time_str(minutes_value: float) -> str:
    """把分钟值转回 HH:MM。"""
    day_minutes = 1440
    normalized = ((minutes_value % day_minutes) + day_minutes) % day_minutes
    hour = int(normalized // 60)
    minute = int(normalized % 60)
    return f"{hour:02d}:{minute:02d}"


def get_distance_and_duration(from_id: str, to_id: str, dist_payload: Dict) -> Tuple[Optional[float], Optional[float]]:
    """获取两点间真实里程和时长。

    读取顺序：
    1. payload 内传进来的距离矩阵
    2. 本地进程缓存
    3. 数据库 `store_distance_matrix`
    """
    if not from_id or not to_id:
        return None, None
    if from_id == to_id:
        return 0.0, 0.0

    node_map = dist_payload.get(from_id) if isinstance(dist_payload, dict) else None
    if isinstance(node_map, dict):
        raw = node_map.get(to_id)
        if isinstance(raw, dict):
            distance_km = float(raw.get("distance_km") or raw.get("distanceKm") or 0.0)
            duration_min = float(raw.get("duration_minutes") or raw.get("durationMinutes") or 0.0)
            if distance_km > 0:
                return distance_km, max(0.0, duration_min)
        elif raw not in (None, "", 0):
            return float(raw), None

    cache_key = (str(from_id), str(to_id))
    cached = _DISTANCE_CACHE.get(cache_key)
    if cached:
        return cached

    try:
        with _get_db_connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT distance_km, duration_minutes
                    FROM store_distance_matrix
                    WHERE from_store_id=%s AND to_store_id=%s
                    """,
                    (from_id, to_id),
                )
                row = cursor.fetchone()
    except Exception as exc:
        _log(f"查询距离矩阵失败 {from_id}->{to_id}: {exc}")
        return None, None

    if not row:
        return None, None

    result = (
        float(row.get("distance_km") or 0.0),
        float(row.get("duration_minutes") or 0.0),
    )
    _DISTANCE_CACHE[cache_key] = result
    return result


def get_distance(from_id: str, to_id: str, dist_payload: Dict) -> Optional[float]:
    """只取里程。"""
    distance_km, _ = get_distance_and_duration(from_id, to_id, dist_payload)
    return distance_km


def get_travel_minutes(from_id: str, to_id: str, vehicle_speed: float, dist_payload: Dict) -> Optional[float]:
    """计算或读取行驶分钟数。

    优先使用数据库里的真实 `duration_minutes`；
    只有数据库没有时长时，才退回“里程 / 车速”的标准折算。
    """
    distance_km, duration_min = get_distance_and_duration(from_id, to_id, dist_payload)
    if distance_km is None:
        return None
    if duration_min and duration_min > 0:
        return float(duration_min)
    speed = max(1.0, float(vehicle_speed or SPEED_DEFAULT))
    return float(distance_km) / speed * 60.0


def load_stores_from_payload(payload: Dict, wave: Dict) -> Dict[str, Dict[str, Any]]:
    """优先从 payload 直接取门店。

    这条分支主要服务推演场景，因为推演会把修改后的门店时间直接塞进 payload。
    """
    stores_list = payload.get("stores") if isinstance(payload, dict) else None
    if not isinstance(stores_list, list) or not stores_list:
        return {}

    wave_id = str(wave.get("waveId", "")).upper().strip()
    wave_no = wave_id[1] if wave_id.startswith("W") and len(wave_id) >= 2 else ""
    start_min = float(wave.get("startMin", 0) or 0)
    stores: Dict[str, Dict[str, Any]] = {}

    for item in stores_list:
        if not isinstance(item, dict):
            continue
        code = str(item.get("id") or item.get("shop_code") or item.get("code") or "").strip()
        if not code:
            continue

        belongs_text = str(item.get("waveBelongs") or item.get("wave_belongs") or "").strip()
        if belongs_text and wave_no:
            belongs_list = [segment.strip() for segment in belongs_text.split(",") if segment.strip()]
            if wave_no not in belongs_list:
                continue

        boxes_raw = item.get("boxes")
        if boxes_raw in (None, ""):
            boxes_raw = item.get("total_resolved_load")
        boxes = float(boxes_raw or 0.0)

        desired_min = item.get("desiredArrivalMin")
        if desired_min in (None, ""):
            desired_time = str(
                item.get("desiredArrival")
                or item.get("first_wave_time")
                or item.get("second_wave_time")
                or item.get("arrival_time_w3")
                or item.get("arrival_time_w4")
                or ""
            ).strip()
            desired_min = time_str_to_minutes(desired_time, start_min) if desired_time else start_min
        desired_min = float(desired_min or start_min)

        allowed_late = item.get("parking")
        if allowed_late in (None, ""):
            allowed_late = item.get("allowedLateMinutes")
        if allowed_late in (None, ""):
            allowed_late = item.get("allowed_late_minutes")
        if allowed_late in (None, ""):
            allowed_late = 15

        latest_allowed = item.get("latestAllowedArrivalMin")
        if latest_allowed in (None, ""):
            latest_allowed = desired_min + float(allowed_late or 0)

        service_minutes = float(item.get("serviceMinutes") or item.get("service_minutes") or 8)
        actual_service = float(item.get("actualServiceMinutes") or service_minutes)

        stores[code] = {
            "id": code,
            "name": str(item.get("name") or item.get("shop_name") or code),
            "boxes": boxes,
            "desiredArrivalMin": desired_min,
            "latestAllowedArrivalMin": float(latest_allowed),
            "serviceMinutes": int(service_minutes),
            "actualServiceMinutes": actual_service,
            "difficulty": float(item.get("difficulty") or 1.0),
            "parking": int(float(allowed_late or 15)),
            "waveBelongs": belongs_text,
        }

    if stores:
        _log(f"加载门店 {len(stores)} 家（波次 {wave_id}，来源 payload）")
    return stores


def load_stores_from_db(wave: Dict) -> Dict[str, Dict[str, Any]]:
    """从标准求解调用表加载门店。

    这是主调度和推演共用的真实门店入口。
    """
    wave_id = str(wave.get("waveId") or "").upper()
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

    wave_no = wave_id[1]
    with _get_db_connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                SELECT shop_code, wave_belongs, {load_field} AS boxes, {time_field} AS desired_time
                FROM store_wave_load_resolved
                WHERE FIND_IN_SET(%s, wave_belongs) > 0
                """,
                (wave_no,),
            )
            resolved_rows = cursor.fetchall() or []
            if not resolved_rows:
                _log(f"store_wave_load_resolved 中无波次 {wave_id} 的门店")
                return {}

            shop_codes = [row["shop_code"] for row in resolved_rows]
            placeholders = ",".join(["%s"] * len(shop_codes))
            cursor.execute(
                f"""
                SELECT shop_code, shop_name, service_minutes, allowed_late_minutes, difficulty
                FROM C_SHOP_MAIN
                WHERE shop_code IN ({placeholders})
                """,
                tuple(shop_codes),
            )
            shop_rows = cursor.fetchall() or []

    shop_map = {row["shop_code"]: row for row in shop_rows}
    stores: Dict[str, Dict[str, Any]] = {}

    for row in resolved_rows:
        code = str(row.get("shop_code") or "").strip()
        boxes = float(row.get("boxes") or 0.0)
        if not code or boxes <= 0:
            continue

        desired_text = str(row.get("desired_time") or "").strip()
        desired_min = time_str_to_minutes(desired_text, float(wave.get("startMin", 0) or 0))

        shop_info = shop_map.get(code)
        if not shop_info:
            _log(f"门店 {code} 在 C_SHOP_MAIN 中不存在，已跳过")
            continue

        service_minutes = shop_info.get("service_minutes")
        allowed_late = shop_info.get("allowed_late_minutes")
        difficulty = shop_info.get("difficulty")
        if service_minutes is None or allowed_late is None or difficulty is None:
            _log(f"门店 {code} 的服务时长/允许晚到/难度字段缺失，已跳过")
            continue

        stores[code] = {
            "id": code,
            "name": str(shop_info.get("shop_name") or code),
            "boxes": boxes,
            "desiredArrivalMin": desired_min,
            "latestAllowedArrivalMin": desired_min + float(allowed_late or 0),
            "serviceMinutes": int(service_minutes),
            "actualServiceMinutes": float(service_minutes) * max(0.1, float(difficulty)),
            "difficulty": float(difficulty),
            "parking": int(float(allowed_late or 15)),
            "waveBelongs": str(row.get("wave_belongs") or ""),
        }

    _log(f"加载门店 {len(stores)} 家（波次 {wave_id}）")
    return stores


def compute_one_way_distance(route: List[str], dist_payload: Dict) -> float:
    """计算线路单程里程。"""
    if not route:
        return 0.0
    total = 0.0
    current = "DC"
    for store_id in route:
        leg_distance = get_distance(current, store_id, dist_payload)
        if leg_distance is None:
            return float("inf")
        total += leg_distance
        current = store_id
    return total


def is_route_feasible(
    vehicle: Dict,
    route: List[str],
    stores: Dict,
    dist_payload: Dict,
    wave: Dict,
    scenario: Dict,
    prior_stats: Optional[Dict] = None,
) -> Tuple[bool, str]:
    """按真实业务约束检查线路可行性。"""
    wave_id = str(wave.get("waveId") or "").upper()
    speed = SPEED_W3 if wave_id == "W3" else SPEED_DEFAULT
    vehicle_speed = float(vehicle.get("speed") or speed)

    start_min = max(
        float(wave.get("startMin") or 0),
        float(vehicle.get("earliestDepartureMin") or wave.get("startMin") or 0),
        float(scenario.get("dispatchStartMin") or wave.get("startMin") or 0),
    )
    if wave_id in ("W1", "W2") and prior_stats and "finish_time" in prior_stats:
        start_min = max(start_min, float(prior_stats.get("finish_time") or 0))

    current_time = start_min
    current_node = "DC"
    cumulative_load = 0.0
    one_way_distance = 0.0

    for store_id in route:
        store = stores.get(store_id)
        if not store:
            return False, f"门店 {store_id} 不存在"

        cumulative_load += float(store.get("boxes") or 0.0)
        if cumulative_load > MAX_CAPACITY:
            return False, f"容量超限 {cumulative_load:.3f} > {MAX_CAPACITY}"

        travel_min = get_travel_minutes(current_node, store_id, vehicle_speed, dist_payload)
        if travel_min is None:
            return False, f"距离缺失 {current_node}->{store_id}"

        arrival = current_time + travel_min
        desired = float(store.get("desiredArrivalMin") or start_min)
        latest_allowed = float(store.get("latestAllowedArrivalMin") or desired)
        if desired < start_min:
            desired += 1440
            latest_allowed += 1440

        if arrival > latest_allowed:
            return False, f"时间窗违反 {minutes_to_time_str(arrival)} > {minutes_to_time_str(latest_allowed)}"

        leg_distance, _ = get_distance_and_duration(current_node, store_id, dist_payload)
        one_way_distance += float(leg_distance or 0.0)
        current_time = arrival + float(store.get("actualServiceMinutes") or 0)
        current_node = store_id

    back_travel_min = get_travel_minutes(current_node, "DC", vehicle_speed, dist_payload)
    if back_travel_min is None:
        return False, f"距离缺失 {current_node}->DC"
    back_distance, _ = get_distance_and_duration(current_node, "DC", dist_payload)
    round_distance = one_way_distance + float(back_distance or 0.0)
    finish_time = current_time + back_travel_min
    service_end = current_time

    if wave_id in ("W1", "W2"):
        if len(route) > MAX_STOPS_PER_TRIP_W1W2:
            return False, f"门店数超限 {len(route)} > {MAX_STOPS_PER_TRIP_W1W2}"
        prior_round = float((prior_stats or {}).get("prior_round_distance") or 0.0)
        if prior_round + round_distance > MAX_MILEAGE_W1W2_TOTAL:
            return False, f"里程超限 {prior_round + round_distance:.1f} > {MAX_MILEAGE_W1W2_TOTAL}"
    elif wave_id == "W3":
        if one_way_distance > MAX_MILEAGE_W3_ONE_WAY:
            return False, f"W3单程超限 {one_way_distance:.1f} > {MAX_MILEAGE_W3_ONE_WAY}"

    wave_end_min = float(wave.get("endMin") or 0)
    wave_end_mode = str(wave.get("endMode") or "return")
    wave_relax_end = bool(wave.get("relaxEnd"))
    if wave_end_min > 0 and not wave_relax_end:
        compare_time = finish_time if wave_end_mode == "return" else service_end
        if compare_time > wave_end_min:
            label = "回库时间" if wave_end_mode == "return" else "服务结束"
            return False, f"波次结束超时 {label} {minutes_to_time_str(compare_time)} > {minutes_to_time_str(wave_end_min)}"

    return True, ""


def compute_route_cost(
    vehicle: Dict,
    route: List[str],
    stores: Dict,
    dist_payload: Dict,
    wave: Dict,
    scenario: Dict,
    prior_stats: Optional[Dict] = None,
) -> float:
    """计算线路代价。

    当前目标是“里程 + 轻度迟到惩罚”，越小越好。
    """
    wave_id = str(wave.get("waveId") or "").upper()
    speed = SPEED_W3 if wave_id == "W3" else SPEED_DEFAULT
    vehicle_speed = float(vehicle.get("speed") or speed)

    start_min = max(
        float(wave.get("startMin") or 0),
        float(vehicle.get("earliestDepartureMin") or wave.get("startMin") or 0),
        float(scenario.get("dispatchStartMin") or wave.get("startMin") or 0),
    )
    if wave_id in ("W1", "W2") and prior_stats and "finish_time" in prior_stats:
        start_min = max(start_min, float(prior_stats.get("finish_time") or 0))

    current_time = start_min
    current_node = "DC"
    total_distance = 0.0
    total_lateness = 0.0

    for store_id in route:
        store = stores.get(store_id)
        if not store:
            return 1e9
        travel_min = get_travel_minutes(current_node, store_id, vehicle_speed, dist_payload)
        if travel_min is None:
            return 1e9
        arrival = current_time + travel_min
        desired = float(store.get("desiredArrivalMin") or start_min)
        if desired < start_min:
            desired += 1440
        total_lateness += max(0.0, arrival - desired)
        leg_distance, _ = get_distance_and_duration(current_node, store_id, dist_payload)
        total_distance += float(leg_distance or 0.0)
        current_time = arrival + float(store.get("actualServiceMinutes") or 0.0)
        current_node = store_id

    back_distance, _ = get_distance_and_duration(current_node, "DC", dist_payload)
    total_distance += float(back_distance or 0.0)
    return total_distance + total_lateness * 0.5


def compute_route_with_times(
    vehicle: Dict,
    route: List[str],
    stores: Dict,
    dist_payload: Dict,
    wave: Dict,
    scenario: Dict,
    prior_stats: Optional[Dict] = None,
) -> Tuple[float, float, List[Dict[str, Any]], bool, str]:
    """把线路展开成逐店时刻和逐段里程。"""
    wave_id = str(wave.get("waveId") or "").upper()
    speed = SPEED_W3 if wave_id == "W3" else SPEED_DEFAULT
    vehicle_speed = float(vehicle.get("speed") or speed)

    start_min = max(
        float(wave.get("startMin") or 0),
        float(vehicle.get("earliestDepartureMin") or wave.get("startMin") or 0),
        float(scenario.get("dispatchStartMin") or wave.get("startMin") or 0),
    )
    if wave_id in ("W1", "W2") and prior_stats and "finish_time" in prior_stats:
        start_min = max(start_min, float(prior_stats.get("finish_time") or 0))

    current_time = start_min
    current_node = "DC"
    total_distance = 0.0
    total_load = 0.0
    cumulative_load = 0.0
    one_way_distance = 0.0
    stops_info: List[Dict[str, Any]] = []

    for seq, store_id in enumerate(route, start=1):
        store = stores.get(store_id)
        if not store:
            return 0.0, 0.0, [], False, f"门店 {store_id} 不存在"

        boxes = float(store.get("boxes") or 0.0)
        cumulative_load += boxes
        if cumulative_load > MAX_CAPACITY:
            return 0.0, 0.0, [], False, f"容量超限 {cumulative_load:.3f} > {MAX_CAPACITY}"

        leg_distance, leg_duration = get_distance_and_duration(current_node, store_id, dist_payload)
        if leg_distance is None:
            return 0.0, 0.0, [], False, f"距离缺失 {current_node}->{store_id}"

        travel_min = leg_duration if leg_duration and leg_duration > 0 else get_travel_minutes(current_node, store_id, vehicle_speed, dist_payload)
        if travel_min is None:
            return 0.0, 0.0, [], False, f"时长缺失 {current_node}->{store_id}"

        arrival = current_time + travel_min
        desired = float(store.get("desiredArrivalMin") or start_min)
        latest_allowed = float(store.get("latestAllowedArrivalMin") or desired)
        if desired < start_min:
            desired += 1440
            latest_allowed += 1440
        if arrival > latest_allowed:
            return 0.0, 0.0, [], False, f"时间窗违反 {minutes_to_time_str(arrival)} > {minutes_to_time_str(latest_allowed)}"

        leave = arrival + float(store.get("actualServiceMinutes") or 0.0)
        total_distance += float(leg_distance or 0.0)
        one_way_distance += float(leg_distance or 0.0)
        total_load = cumulative_load
        stops_info.append(
            {
                "seq": seq,
                "store_id": store_id,
                "store_name": str(store.get("name") or store_id),
                "boxes": boxes,
                "cumulative_load": cumulative_load,
                "expected_min": desired,
                "expected_time_str": minutes_to_time_str(desired),
                "arrival_min": arrival,
                "arrival_time_str": minutes_to_time_str(arrival),
                "leave_min": leave,
                "leave_time_str": minutes_to_time_str(leave),
                "leg_distance_km": float(leg_distance or 0.0),
                "leg_duration_min": float(travel_min or 0.0),
                "from_node": current_node,
            }
        )
        current_time = leave
        current_node = store_id

    back_travel_min = get_travel_minutes(current_node, "DC", vehicle_speed, dist_payload)
    back_distance, _ = get_distance_and_duration(current_node, "DC", dist_payload)
    if back_travel_min is None:
        return 0.0, 0.0, [], False, f"距离缺失 {current_node}->DC"

    total_distance += float(back_distance or 0.0)
    finish_time = current_time + back_travel_min
    service_end = current_time

    if wave_id in ("W1", "W2"):
        prior_round = float((prior_stats or {}).get("prior_round_distance") or 0.0)
        if prior_round + total_distance > MAX_MILEAGE_W1W2_TOTAL:
            return 0.0, 0.0, [], False, f"总里程超限 {prior_round + total_distance:.1f} > {MAX_MILEAGE_W1W2_TOTAL}"
        if len(route) > MAX_STOPS_PER_TRIP_W1W2:
            return 0.0, 0.0, [], False, f"门店数超限 {len(route)} > {MAX_STOPS_PER_TRIP_W1W2}"
    elif wave_id == "W3" and one_way_distance > MAX_MILEAGE_W3_ONE_WAY:
        return 0.0, 0.0, [], False, f"W3单程超限 {one_way_distance:.1f} > {MAX_MILEAGE_W3_ONE_WAY}"

    wave_end_min = float(wave.get("endMin") or 0)
    wave_end_mode = str(wave.get("endMode") or "return")
    wave_relax_end = bool(wave.get("relaxEnd"))
    if wave_end_min > 0 and not wave_relax_end:
        compare_time = finish_time if wave_end_mode == "return" else service_end
        if compare_time > wave_end_min:
            label = "回库时间" if wave_end_mode == "return" else "服务结束"
            return 0.0, 0.0, [], False, f"波次结束超时 {label} {minutes_to_time_str(compare_time)} > {minutes_to_time_str(wave_end_min)}"

    return total_distance, total_load, stops_info, True, ""


def compute_w1_finish_times_from_routes(
    w1_routes_by_plate: Dict[str, List[List[str]]],
    stores: Dict,
    vehicles: List,
    dist_payload: Dict,
    wave: Dict,
    scenario: Dict,
) -> Dict[str, Dict]:
    """把 W1 已有线路换算成 W2 的前序占用信息。"""
    vehicle_map = {str(item.get("plateNo") or ""): item for item in (vehicles or []) if isinstance(item, dict)}
    result: Dict[str, Dict] = {}
    for plate, routes in (w1_routes_by_plate or {}).items():
        vehicle = vehicle_map.get(str(plate))
        if not vehicle:
            continue
        total_distance = 0.0
        finish_time = float(wave.get("startMin") or 0)
        route_count = 0
        for route in routes or []:
            if not route:
                continue
            route_distance, _, stops_info, feasible, _ = compute_route_with_times(
                vehicle, route, stores, dist_payload, wave, scenario, {}
            )
            if not feasible:
                continue
            route_count += 1
            total_distance += route_distance
            if stops_info:
                finish_time = max(finish_time, float(stops_info[-1].get("leave_min") or finish_time))
                back_travel = get_travel_minutes(stops_info[-1].get("store_id"), "DC", float(vehicle.get("speed") or SPEED_DEFAULT), dist_payload)
                if back_travel is not None:
                    finish_time += back_travel
        result[str(plate)] = {
            "finish_time": finish_time,
            "prior_round_distance": total_distance,
            "priorWaveCount": route_count,
        }
    return result


def compute_w1_finish_times(
    w1_assignments: Dict[str, List[str]],
    stores: Dict,
    vehicles: List,
    dist_payload: Dict,
    wave: Dict,
    scenario: Dict,
) -> Dict[str, Dict]:
    """兼容旧格式的 W1 -> W2 接力输入。"""
    routes_by_plate = {}
    for plate, store_ids in (w1_assignments or {}).items():
        if store_ids:
            routes_by_plate[str(plate)] = [list(store_ids)]
    return compute_w1_finish_times_from_routes(routes_by_plate, stores, vehicles, dist_payload, wave, scenario)


def output_detail(wave_id: str, state: List[Dict], stores: Dict, dist_payload: Dict, wave: Dict, scenario: Dict, prior_stats_by_plate: Dict) -> None:
    """把最优结果输出成 TXT，给业务和测试侧做人工核对。"""
    filename = f"C:\\Users\\laoj0\\Desktop\\aco_{wave_id}.txt"
    try:
        with open(filename, "w", encoding="utf-8") as handler:
            handler.write(f"========== 波次 {wave_id} ACO 调度明细 ==========\n")
            handler.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            total_assigned = 0
            for vehicle in state or []:
                plate = vehicle.get("plateNo")
                routes = vehicle.get("routes") if isinstance(vehicle.get("routes"), list) else []
                if not plate or not routes:
                    continue
                prior_stats = prior_stats_by_plate.get(plate, {})
                for route_index, route in enumerate(routes, start=1):
                    if not route:
                        continue
                    total_assigned += len(route)
                    total_dist, total_load, stops_info, feasible, reason = compute_route_with_times(
                        vehicle, route, stores, dist_payload, wave, scenario, prior_stats
                    )
                    if not feasible:
                        handler.write(f"车辆 {plate} 线路{route_index} 不可行: {reason}\n\n")
                        continue

                    handler.write(
                        f"车辆 {plate} | 第{route_index}趟 | 门店数 {len(route)} | 总里程 {total_dist:.1f} km | 总装载 {total_load:.3f}\n"
                    )
                    handler.write("序号\t店铺编号\t店铺名称\t货量\t累计装载\t期望到店\t调度到店\t离店时间\t增量里程\n")
                    for stop in stops_info:
                        handler.write(
                            f"{stop['seq']}\t{stop['store_id']}\t{stop['store_name']}\t{stop['boxes']:.3f}\t"
                            f"{stop['cumulative_load']:.3f}\t{stop['expected_time_str']}\t{stop['arrival_time_str']}\t"
                            f"{stop['leave_time_str']}\t{stop['leg_distance_km']:.1f}\n"
                        )
                    handler.write("\n")

            handler.write("========== 调度统计 ==========\n")
            handler.write(f"总门店数: {len(stores)}\n")
            handler.write(f"已调度门店数: {total_assigned}\n")
            handler.write(f"未调度门店数: {len(stores) - total_assigned}\n")

        _log(f"调度结果已写入 {filename}，已调度 {total_assigned}/{len(stores)} 家")
    except Exception as exc:
        _log(f"写入结果文件失败: {exc}")


def build_best_state_detailed(state: List[Dict], stores: Dict, dist_payload: Dict, wave: Dict, scenario: Dict, prior_stats_by_plate: Dict) -> List[Dict]:
    """把内部最优解转成前端/推演/落库统一的详细结构。"""
    detailed: List[Dict] = []
    for vehicle in state or []:
        if not isinstance(vehicle, dict):
            continue
        plate = str(vehicle.get("plateNo") or "").strip()
        if not plate:
            continue

        vehicle_detail = {
            "plateNo": plate,
            "capacity": float(vehicle.get("capacity") or 0.0),
            "speed": float(vehicle.get("speed") or 0.0),
            "routes": [],
        }

        routes = vehicle.get("routes") if isinstance(vehicle.get("routes"), list) else []
        prior_stats = prior_stats_by_plate.get(plate, {}) if isinstance(prior_stats_by_plate, dict) else {}

        for route_index, route in enumerate(routes, start=1):
            if not isinstance(route, list) or not route:
                continue
            total_dist, total_load, stops_info, feasible, reason = compute_route_with_times(
                vehicle, route, stores, dist_payload, wave, scenario, prior_stats
            )
            stop_rows = []
            for stop in stops_info or []:
                stop_rows.append(
                    {
                        "shop_code": str(stop.get("store_id") or ""),
                        "shop_name": str(stop.get("store_name") or ""),
                        "arrivalMin": float(stop.get("arrival_min") or 0.0),
                        "planned_arrival_time": str(stop.get("arrival_time_str") or ""),
                        "expected_time_str": str(stop.get("expected_time_str") or ""),
                        "leave_time_str": str(stop.get("leave_time_str") or ""),
                        "boxes": float(stop.get("boxes") or 0.0),
                        "route_leg_km": float(stop.get("leg_distance_km") or 0.0),
                        "route_leg_text": f"+{float(stop.get('leg_distance_km') or 0.0):.1f} km",
                        "scheduled_source": "solver",
                    }
                )

            vehicle_detail["routes"].append(
                {
                    "tripNo": route_index,
                    "route": [str(store_id or "") for store_id in route if str(store_id or "").strip()],
                    "routeDistanceKm": float(total_dist or 0.0),
                    "totalLoad": float(total_load or 0.0),
                    "stops": stop_rows,
                    "feasible": bool(feasible),
                    "reason": str(reason or ""),
                }
            )
        detailed.append(vehicle_detail)
    return detailed


class AntColonyOptimizer:
    """轻量蚁群优化器。

    这里保留项目原有“按车构造、逐店插入、整轮评估”的结构，
    让前后端接口保持稳定，同时便于日志审计。
    """

    def __init__(
        self,
        stores: Dict,
        dist_payload: Dict,
        wave: Dict,
        scenario: Dict,
        vehicles: List,
        w1_prior_stats: Dict,
        rng: random.Random,
    ):
        self.stores = stores
        self.dist = dist_payload
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
        self.w1_prior_stats = w1_prior_stats
        self.rng = rng
        self.ant_count = 8
        self.max_iterations = 30
        self.q0 = 0.85
        self.evaporation = 0.2
        self.pheromone: Dict[Tuple[str, str], float] = {}

    def _init_pheromone(self, stores_list: List[str]) -> None:
        nodes = ["DC"] + stores_list
        for source in nodes:
            for target in nodes:
                if source != target:
                    self.pheromone[(source, target)] = 1.0

    def _get_pheromone(self, from_id: str, to_id: str) -> float:
        return self.pheromone.get((from_id, to_id), 1.0)

    def _update_pheromone(self, solutions: List[Tuple[List[str], float]]) -> None:
        for key in list(self.pheromone.keys()):
            self.pheromone[key] *= (1 - self.evaporation)
        if not solutions:
            return
        best_route, best_cost = min(solutions, key=lambda item: item[1])
        delta = 1.0 / (best_cost + 1.0)
        current = "DC"
        for store_id in best_route:
            self.pheromone[(current, store_id)] += delta
            current = store_id
        self.pheromone[(current, "DC")] += delta

    def _build_ant_solution(self, initial_routes: Dict[str, List[str]]) -> Tuple[List[Dict], Set[str]]:
        state: List[Dict] = []
        for vehicle in self.vehicles:
            state.append(
                {
                    "plateNo": vehicle.get("plateNo"),
                    "capacity": vehicle.get("capacity", 1.0),
                    "speed": vehicle.get("speed", 38.0),
                    "earliestDepartureMin": vehicle.get("earliestDepartureMin", 0),
                    "routes": [],
                }
            )

        for vehicle in state:
            plate = str(vehicle.get("plateNo") or "")
            if plate in initial_routes and initial_routes[plate]:
                vehicle["routes"] = [initial_routes[plate].copy()]

        unassigned: Set[str] = set(self.stores.keys())
        for vehicle in state:
            for route in vehicle.get("routes", []):
                for store_id in route:
                    unassigned.discard(store_id)

        wave_id = str(self.wave.get("waveId") or "").upper()
        speed = SPEED_W3 if wave_id == "W3" else SPEED_DEFAULT

        for vehicle in state:
            if not unassigned:
                break

            plate = str(vehicle.get("plateNo") or "")
            prior_stats = self.w1_prior_stats.get(plate, {})
            current_route = vehicle["routes"][0] if vehicle["routes"] else []
            vehicle_speed = float(vehicle.get("speed") or speed)
            remaining = sorted(list(unassigned), key=lambda store_id: self.stores[store_id]["desiredArrivalMin"])

            while remaining:
                current_node = current_route[-1] if current_route else "DC"
                candidates: List[Tuple[str, float]] = []
                for store_id in list(remaining):
                    test_route = current_route + [store_id]
                    if wave_id == "W3":
                        test_one_way = compute_one_way_distance(test_route, self.dist)
                        if test_one_way > MAX_MILEAGE_W3_ONE_WAY + 0.1:
                            continue

                    feasible, _ = is_route_feasible(
                        vehicle,
                        test_route,
                        self.stores,
                        self.dist,
                        self.wave,
                        self.scenario,
                        prior_stats,
                    )
                    if not feasible:
                        continue

                    tau = self._get_pheromone(current_node, store_id)
                    leg_distance = get_distance(current_node, store_id, self.dist)
                    if leg_distance is None:
                        continue
                    eta = 1.0 / (leg_distance + 0.1)
                    candidates.append((store_id, tau ** 1.0 * eta ** 2.0))

                if not candidates:
                    break

                if self.rng.random() < self.q0:
                    chosen = max(candidates, key=lambda item: item[1])[0]
                else:
                    total_weight = sum(item[1] for item in candidates)
                    if total_weight <= 0:
                        chosen = candidates[0][0]
                    else:
                        rand_value = self.rng.random() * total_weight
                        acc_weight = 0.0
                        chosen = candidates[-1][0]
                        for store_id, weight in candidates:
                            acc_weight += weight
                            if acc_weight >= rand_value:
                                chosen = store_id
                                break

                current_route.append(chosen)
                remaining.remove(chosen)
                unassigned.remove(chosen)

            if current_route:
                feasible, reason = is_route_feasible(
                    vehicle,
                    current_route,
                    self.stores,
                    self.dist,
                    self.wave,
                    self.scenario,
                    prior_stats,
                )
                if feasible:
                    vehicle["routes"] = [current_route]
                else:
                    _log(f"警告：车辆 {plate} 最终线路不可行: {reason}")

        return state, unassigned

    def optimize(self, initial_routes: Dict[str, List[str]]) -> Tuple[List[Dict], Set[str], float]:
        stores_list = list(self.stores.keys())
        self._init_pheromone(stores_list)

        best_state: Optional[List[Dict]] = None
        best_unassigned: Optional[Set[str]] = None
        best_cost = float("inf")

        for iteration in range(self.max_iterations):
            iteration_start = time.perf_counter()
            solutions: List[Tuple[List[str], float]] = []
            iteration_best_state = None
            iteration_best_unassigned = None
            iteration_best_cost = float("inf")

            for _ in range(self.ant_count):
                state, unassigned = self._build_ant_solution(initial_routes)
                total_distance = 0.0
                for vehicle in state:
                    plate = str(vehicle.get("plateNo") or "")
                    prior = self.w1_prior_stats.get(plate, {})
                    for route in vehicle.get("routes", []):
                        cost = compute_route_cost(
                            vehicle,
                            route,
                            self.stores,
                            self.dist,
                            self.wave,
                            self.scenario,
                            prior,
                        )
                        if cost < 1e8:
                            total_distance += cost

                penalty = len(unassigned) * 10000
                total_cost = total_distance + penalty

                if total_cost < iteration_best_cost:
                    iteration_best_cost = total_cost
                    iteration_best_state = state
                    iteration_best_unassigned = unassigned.copy()

                if total_cost < best_cost:
                    best_cost = total_cost
                    best_state = state
                    best_unassigned = unassigned.copy()
                    best_route: List[str] = []
                    for vehicle in state:
                        for route in vehicle.get("routes", []):
                            best_route.extend(route)
                    if best_route:
                        solutions.append((best_route, total_cost))

            if solutions:
                self._update_pheromone(solutions)

            elapsed_ms = (time.perf_counter() - iteration_start) * 1000
            assigned_count = len(self.stores) - len(iteration_best_unassigned or set())
            _log(
                f"迭代 {iteration + 1}/{self.max_iterations}，已调度 {assigned_count}/{len(self.stores)} 家，"
                f"代价 {iteration_best_cost:.2f}，耗时 {elapsed_ms:.1f} ms"
            )

            if iteration_best_unassigned is not None and len(iteration_best_unassigned) == 0:
                _log(f"第 {iteration + 1} 轮已完成全量调度，提前结束")
                break

        return best_state or [], best_unassigned or set(), best_cost


def solve(payload: Dict) -> Dict[str, Any]:
    """ACO 主入口。"""
    _log("=" * 50)
    _log("蚁群算法 (ACO) 启动 - 固定时速模型，支持 W1 多趟接力")
    _log("=" * 50)

    scenario = payload["scenario"]
    wave = payload["wave"]
    dist_payload = payload.get("dist", {})
    vehicles = payload.get("vehicles", [])
    wave_id = str(wave.get("waveId") or "").upper()
    w1_assignments = payload.get("w1_assignments", {})
    w1_routes_by_plate = payload.get("w1_routes_by_plate", {})
    w1_prior_stats_raw = payload.get("w1_prior_stats", {})

    stores = load_stores_from_payload(payload, wave)
    if not stores:
        stores = load_stores_from_db(wave)
    if not stores:
        _log("没有加载到任何门店，返回空解")
        return {"bestState": [], "bestStateDetailed": [], "traceLog": [], "unassigned": []}

    excluded = payload.get("excluded_vehicles", [])
    if excluded:
        excluded_set = set(excluded)
        vehicles = [vehicle for vehicle in vehicles if vehicle.get("plateNo") not in excluded_set]

    if not vehicles:
        _log("没有可用车辆")
        return {
            "bestState": [],
            "bestStateDetailed": [],
            "traceLog": [],
            "unassigned": list(stores.keys()),
        }

    prior_source = "empty"
    w1_prior_stats: Dict[str, Dict] = {}
    if wave_id == "W2":
        if w1_prior_stats_raw and isinstance(w1_prior_stats_raw, dict):
            prior_source = "payload_prior"
            for plate, item in w1_prior_stats_raw.items():
                if not isinstance(item, dict):
                    continue
                w1_prior_stats[str(plate)] = {
                    "finish_time": float(item.get("finish_time") or 0),
                    "prior_round_distance": float(item.get("prior_round_distance") or 0),
                    "priorWaveCount": int(item.get("priorWaveCount") or 0),
                }
            _log(f"W2 prior source = payload_prior，共 {len(w1_prior_stats)} 辆车")
        elif w1_routes_by_plate and isinstance(w1_routes_by_plate, dict):
            prior_source = "routes_by_plate"
            w1_prior_stats = compute_w1_finish_times_from_routes(
                w1_routes_by_plate, stores, vehicles, dist_payload, wave, scenario
            )
            _log(f"W2 prior source = routes_by_plate，共 {len(w1_prior_stats)} 辆车")
        elif w1_assignments and isinstance(w1_assignments, dict):
            prior_source = "assignments"
            w1_prior_stats = compute_w1_finish_times(
                w1_assignments, stores, vehicles, dist_payload, wave, scenario
            )
            _log(f"W2 prior source = assignments，共 {len(w1_prior_stats)} 辆车")
        else:
            _log("W2 prior source = empty，无接力数据")

        for plate, stats in list(w1_prior_stats.items())[:5]:
            _log(
                f"  车辆 {plate}: finish_time={minutes_to_time_str(stats['finish_time'])}, "
                f"distance={stats['prior_round_distance']:.1f}km, count={stats['priorWaveCount']}"
            )
    else:
        _log(f"{wave_id} prior source = {prior_source}")

    initial_routes: Dict[str, List[str]] = {}
    if wave_id == "W2" and isinstance(w1_routes_by_plate, dict):
        for plate, routes in w1_routes_by_plate.items():
            if routes and len(routes) > 0 and routes[0]:
                initial_routes[str(plate)] = list(routes[0])
    elif wave_id == "W2" and isinstance(w1_assignments, dict):
        for plate, store_ids in w1_assignments.items():
            if store_ids:
                initial_routes[str(plate)] = list(store_ids)

    rng = random.Random(42)
    rng.shuffle(vehicles)

    optimizer = AntColonyOptimizer(stores, dist_payload, wave, scenario, vehicles, w1_prior_stats, rng)
    best_state, unassigned, best_cost = optimizer.optimize(initial_routes)

    assigned_count = len(stores) - len(unassigned)
    _log(f"优化完成，已调度 {assigned_count}/{len(stores)} 家，总代价: {best_cost:.2f}")
    if unassigned:
        _log(f"警告：{len(unassigned)} 家门店未调度")

    output_detail(wave_id, best_state, stores, dist_payload, wave, scenario, w1_prior_stats)
    best_state_detailed = build_best_state_detailed(
        best_state, stores, dist_payload, wave, scenario, w1_prior_stats
    )
    return {
        "bestState": best_state,
        "bestStateDetailed": best_state_detailed,
        "traceLog": [],
        "unassigned": list(unassigned) if unassigned else [],
    }
