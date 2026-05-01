"""推演失败分析器：输出中文失败原因和建议动作。"""

import json
from solver_core.constraints import check_plan_constraints

from .time_adjuster import hhmm_to_minutes, minutes_to_hhmm, suggest_time

DC_ID = "DC"
VALID_WAVES = ("W1", "W2", "W3", "W4")


def _coerce_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _coerce_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _normalize_wave_key(value):
    text = str(value or "").strip().upper()
    if text in VALID_WAVES:
        return text
    if text in ("1", "FIRST"):
        return "W1"
    if text in ("2", "SECOND"):
        return "W2"
    if text in ("3", "THIRD"):
        return "W3"
    if text in ("4", "FOURTH"):
        return "W4"
    return text


def _normalize_store_id(value):
    text = str(value or "").strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _normalize_store_name(store):
    if not isinstance(store, dict):
        return ""
    return str(store.get("name") or store.get("shop_name") or "").strip()


def _normalize_reason(vtype):
    text = str(vtype or "").strip().lower()
    if text == "arrival_window":
        return "arrival_window"
    if text == "wave_end":
        return "wave_end"
    if text in ("max_route_km_single", "night_regular_distance"):
        return "mileage"
    if text == "capacity":
        return "capacity"
    if text in ("slot", "max_stops", "store_missing"):
        return "max_stops"
    return text or "mixed"


def _route_stops_snapshot(route, stores, arrival_map=None):
    snapshot = []
    arrival_map = arrival_map or {}
    for sid in route if isinstance(route, list) else []:
        code = _normalize_store_id(sid)
        store = stores.get(code) or {}
        snapshot.append(
            {
                "shop_code": code,
                "shop_name": _normalize_store_name(store),
                "arrival": arrival_map.get(code),
            }
        )
    return snapshot


def _get_leg_distance(dist, from_id, to_id):
    from_key = _normalize_store_id(from_id)
    to_key = _normalize_store_id(to_id)
    if not isinstance(dist, dict):
        return None
    row = dist.get(from_key)
    if isinstance(row, dict) and to_key in row:
        return _coerce_float(row.get(to_key))
    row = dist.get(str(from_key))
    if isinstance(row, dict) and str(to_key) in row:
        return _coerce_float(row.get(str(to_key)))
    return None


def _route_total_distance(route, dist):
    route_ids = [_normalize_store_id(x) for x in (route if isinstance(route, list) else []) if _normalize_store_id(x)]
    if not route_ids:
        return 0.0
    total = 0.0
    current = DC_ID
    for sid in route_ids:
        leg = _get_leg_distance(dist, current, sid)
        if leg is None:
            return 0.0
        total += _coerce_float(leg)
        current = sid
    back = _get_leg_distance(dist, current, DC_ID)
    if back is not None:
        total += _coerce_float(back)
    return round(total, 1)


def _all_routes_total_distance(routes, dist):
    total = 0.0
    for route in routes if isinstance(routes, list) else []:
        total += _route_total_distance(route, dist)
    return round(total, 1)


def _route_outbound_distance_until_stop(route, dist, stop_index):
    current = DC_ID
    total = 0.0
    for idx, sid in enumerate(route if isinstance(route, list) else []):
        leg = _get_leg_distance(dist, current, sid)
        if leg is None:
            return 0.0
        total += _coerce_float(leg)
        current = _normalize_store_id(sid)
        if idx >= stop_index:
            break
    return round(total, 1)


def _route_total_load(route, stores):
    total = 0.0
    for sid in (route if isinstance(route, list) else []):
        store = stores.get(_normalize_store_id(sid)) or {}
        total += _coerce_float(store.get("boxes"), 0.0)
    return round(total, 3)


def _resolve_relay_max_km(scenario):
    cfg = (scenario or {}).get("strategyConfigResolved") if isinstance(scenario, dict) else {}
    if not isinstance(cfg, dict):
        cfg = {}
    relay_max_km = _coerce_float(cfg.get("w1w2RelayMaxKm"), 240.0)
    return relay_max_km if relay_max_km > 0 else 240.0


def _resolve_capacity_limit(item, scenario):
    cfg = (scenario or {}).get("strategyConfigResolved") if isinstance(scenario, dict) else {}
    if not isinstance(cfg, dict):
        cfg = {}
    capacity_limit = _coerce_float(
        item.get("capacity"),
        _coerce_float(item.get("solveCapacity"), _coerce_float(cfg.get("maxSolveCapacity"), 1.0)),
    )
    return capacity_limit if capacity_limit > 0 else 1.0


def _shift_time_hhmm(base_hhmm, delta_minutes):
    base_min = hhmm_to_minutes(base_hhmm)
    if base_min is None:
        return None
    return minutes_to_hhmm(base_min + int(delta_minutes))


def _normalize_wave_axis(minute_value, wave_start_min, wave_end_min):
    minute = _coerce_float(minute_value)
    start = _coerce_float(wave_start_min)
    end = _coerce_float(wave_end_min)
    overnight = end < start
    normalized_end = end + 1440.0 if overnight else end
    if overnight and minute < start:
        minute += 1440.0
    return minute, start, normalized_end, overnight


def _format_clock_with_day(minute_value):
    minute = int(round(_coerce_float(minute_value)))
    day = 0
    if minute >= 0:
        day = minute // 1440
    hhmm = minutes_to_hhmm(minute)
    if day <= 0:
        return hhmm
    return f"D+{day} {hhmm}"


def _build_wave_end_detail(item, route, stores, wave, wave_id, sid, violation):
    start_min = _coerce_float((wave or {}).get("startMin"), 0.0)
    end_min = _coerce_float((wave or {}).get("endMin"), 0.0)
    raw_wave_end = _coerce_float(violation.get("waveEndMin"), end_min)
    finish_min = _coerce_float(violation.get("finishMin"), 0.0)
    service_end_min = _coerce_float(violation.get("serviceEndMin"), finish_min)
    end_mode = str(violation.get("endMode") or (wave or {}).get("endMode") or "return").strip().lower()
    actual_end_min = service_end_min if end_mode == "service" else finish_min

    normalized_actual, _, normalized_cutoff, _ = _normalize_wave_axis(actual_end_min, start_min, raw_wave_end)
    over_minutes = max(0, int(round(normalized_actual - normalized_cutoff)))
    if isinstance(violation, dict) and violation.get("waveLateMinutes") is not None:
        over_minutes = max(over_minutes, int(round(_coerce_float(violation.get("waveLateMinutes"), 0.0))))

    current_time = minutes_to_hhmm(_coerce_float((stores.get(sid) or {}).get("desiredArrivalMin")))
    suggested_store_time = _shift_time_hhmm(current_time, -over_minutes) if over_minutes > 0 else current_time
    suggested_wave_end_abs = normalized_cutoff + over_minutes
    suggested_wave_end = _format_clock_with_day(suggested_wave_end_abs)
    target_wave = "W2" if _normalize_wave_key((wave or {}).get("waveId") or wave_id) == "W1" else "W1"

    return {
        "target_vehicle": str(item.get("plateNo") or item.get("vehicleId") or "").strip(),
        "wave_id": _normalize_wave_key((wave or {}).get("waveId") or wave_id),
        "failure_type": "wave_end",
        "violation_type": "wave_end",
        "current_time": current_time,
        "expected_arrival": _format_clock_with_day(normalized_actual),
        "latest_allowed": minutes_to_hhmm(_coerce_float((stores.get(sid) or {}).get("latestAllowedArrivalMin"))),
        "late_minutes": over_minutes,
        "route_stops": _route_stops_snapshot(route, stores),
        "wave_end_time": _format_clock_with_day(normalized_cutoff),
        "actual_finish_time": _format_clock_with_day(normalized_actual),
        "over_minutes": over_minutes,
        "suggested_wave_end_time": suggested_wave_end,
        "suggested_time": suggested_store_time,
        "suggestion_type": "wave_end_limit",
        "suggestion_text": (
            f"建议A：将该波次截止从 {_format_clock_with_day(normalized_cutoff)} 放宽到 {suggested_wave_end}（+{over_minutes}分钟）；"
            f"建议B：店铺时间提前到 {suggested_store_time}（-{over_minutes}分钟）；"
            f"建议C：尝试改派 {target_wave}（若允许跨波次）。"
        ),
    }


def _build_non_arrival_detail(item, routes, stores, dist, wave, scenario, wave_id, sid, failure_type, violation):
    route_index = _coerce_int(violation.get("routeIndex"), 0)
    base_route = routes[route_index] if 0 <= route_index < len(routes) else []
    route_distance = _route_total_distance(base_route, dist)
    all_routes_distance = _all_routes_total_distance(routes, dist)
    route_load = _route_total_load(base_route, stores)
    raw_violation_type = str(violation.get("type") or "").strip().lower()

    detail = {
        "target_vehicle": str(item.get("plateNo") or item.get("vehicleId") or "").strip(),
        "wave_id": _normalize_wave_key((wave or {}).get("waveId") or wave_id),
        "failure_type": failure_type,
        "violation_type": raw_violation_type,
        "current_time": minutes_to_hhmm(_coerce_float((stores.get(sid) or {}).get("desiredArrivalMin"))),
        "expected_arrival": None,
        "latest_allowed": minutes_to_hhmm(_coerce_float((stores.get(sid) or {}).get("latestAllowedArrivalMin"))),
        "late_minutes": 0,
        "route_stops": _route_stops_snapshot(base_route, stores),
        "route_distance_km": route_distance,
        "route_load": route_load,
        "suggestion_type": "info",
        "suggestion_text": "当前失败类型暂不支持自动建议。",
    }

    if failure_type == "wave_end":
        return _build_wave_end_detail(item, base_route, stores, wave, wave_id, sid, violation)

    if failure_type == "mileage":
        relay_limit = _resolve_relay_max_km(scenario)
        current_limit_km = relay_limit
        required_limit_km = route_distance
        if raw_violation_type == "night_regular_distance":
            prior_regular_distance = _coerce_float(item.get("priorRegularDistance"), 0.0)
            required_limit_km = round(prior_regular_distance + all_routes_distance, 1)
        elif raw_violation_type == "max_route_km_single":
            cfg = (scenario or {}).get("strategyConfigResolved") if isinstance(scenario, dict) else {}
            if not isinstance(cfg, dict):
                cfg = {}
            current_limit_km = max(0.0, _coerce_float(cfg.get("w3OneWayMaxKm"), 260.0)) or 260.0
            stop_index = _coerce_int(violation.get("stopIndex"), max(0, len(base_route) - 1))
            required_limit_km = _route_outbound_distance_until_stop(base_route, dist, stop_index)
        required_limit_km = round(required_limit_km, 1)
        current_limit_km = round(current_limit_km, 1)
        exceed_km = max(0.0, round(required_limit_km - current_limit_km, 1))
        detail.update(
            {
                "suggestion_type": "mileage_limit",
                "current_limit_km": current_limit_km,
                "required_limit_km": required_limit_km,
                "exceed_km": exceed_km,
                "suggestion_text": (
                    f"建议A：将线路里程上限从 {current_limit_km:.1f} km 放宽到 {required_limit_km:.1f} km（+{exceed_km:.1f} km）；"
                    "建议B：拆分该线路中的远端门店；"
                    "建议C：改派到下一波次或备用车辆。"
                ),
            }
        )
    elif failure_type == "capacity":
        capacity_limit = _resolve_capacity_limit(item, scenario)
        exceed_load = max(0.0, round(route_load - capacity_limit, 3))
        detail.update(
            {
                "suggestion_type": "capacity_limit",
                "current_capacity_limit": capacity_limit,
                "required_capacity_limit": route_load,
                "exceed_load": exceed_load,
                "suggestion_text": (
                    f"建议A：将装载上限从 {capacity_limit:.3f} 放宽到 {route_load:.3f}（+{exceed_load:.3f}）；"
                    "建议B：将该店拆分到另一辆车；"
                    "建议C：切换更大车型。"
                ),
            }
        )
    elif failure_type == "max_stops":
        current_limit = max(0, len(base_route) - 1)
        required_stops = len(base_route)
        detail.update(
            {
                "suggestion_type": "max_stops",
                "current_stops_limit": current_limit,
                "required_stops_limit": required_stops,
                "suggestion_text": (
                    f"建议A：将单线路最大停靠门店数从 {current_limit} 放宽到 {required_stops}；"
                    "建议B：拆分线路；"
                    "建议C：改派下一波次。"
                ),
            }
        )
    return detail


def _simulate_candidate_timing(item, routes, stores, dist, wave, scenario, target_sid):
    wave_id = _normalize_wave_key((wave or {}).get("waveId"))
    start_min = max(
        _coerce_int((wave or {}).get("startMin")),
        _coerce_int((item or {}).get("earliestDepartureMin"), _coerce_int((wave or {}).get("startMin"))),
    )
    available_time = start_min
    speed = max(_coerce_float((item or {}).get("speed"), 1.0), 1.0)

    for route in routes if isinstance(routes, list) else []:
        if not isinstance(route, list) or not route:
            continue
        current_node = DC_ID
        current_time = available_time
        route_arrival_map = {}
        for raw_sid in route:
            sid = _normalize_store_id(raw_sid)
            store = stores.get(sid)
            if not isinstance(store, dict):
                return None
            leg_distance = _get_leg_distance(dist, current_node, sid)
            if leg_distance is None:
                return None
            arrival = current_time + leg_distance / speed * 60.0
            desired = _coerce_float(store.get("desiredArrivalMin"))
            latest_allowed = _coerce_float(store.get("latestAllowedArrivalMin"))
            if desired < start_min:
                desired += 1440.0
                latest_allowed += 1440.0
            route_arrival_map[sid] = minutes_to_hhmm(arrival)
            if sid == target_sid:
                late_minutes = max(0, int(round(arrival - latest_allowed)))
                suggested = suggest_time(minutes_to_hhmm(desired), late_minutes, buffer_minutes=5)
                target_wave = "W2" if wave_id == "W1" else "W1"
                return {
                    "target_vehicle": str((item or {}).get("plateNo") or (item or {}).get("vehicleId") or "").strip(),
                    "wave_id": wave_id,
                    "failure_type": "arrival_window",
                    "violation_type": "arrival_window",
                    "current_time": minutes_to_hhmm(desired),
                    "expected_arrival": minutes_to_hhmm(arrival),
                    "latest_allowed": minutes_to_hhmm(latest_allowed),
                    "late_minutes": late_minutes,
                    "route_stops": _route_stops_snapshot(route, stores, route_arrival_map),
                    "suggested_time": suggested,
                    "suggestion_type": "time_adjustment",
                    "suggestion_text": (
                        f"建议A：将门店时间调整到 {suggested}；"
                        f"建议B：门店至少提前 {late_minutes} 分钟；"
                        f"建议C：尝试改派 {target_wave}（若允许跨波次）。"
                    ),
                }
            service_minutes = _coerce_float(store.get("actualServiceMinutes") or store.get("serviceMinutes"), 0.0)
            current_time = arrival + service_minutes
            current_node = sid
        available_time = current_time
    return None


def _extract_assigned_store_ids(best_state):
    assigned = set()
    for item in best_state if isinstance(best_state, list) else []:
        if not isinstance(item, dict):
            continue
        for route in item.get("routes") if isinstance(item.get("routes"), list) else []:
            if not isinstance(route, list):
                continue
            for sid in route:
                code = _normalize_store_id(sid)
                if code:
                    assigned.add(code)
    return assigned


def analyze_wave_failures(batch_id, wave_id, payload, solve_result):
    stores_list = payload.get("stores") if isinstance(payload.get("stores"), list) else []
    stores = {}
    for item in stores_list:
        if not isinstance(item, dict):
            continue
        sid = _normalize_store_id(item.get("id"))
        if sid:
            stores[sid] = item

    best_state = solve_result.get("bestState") if isinstance(solve_result, dict) else None
    if best_state is None and isinstance(solve_result, dict):
        best_state = solve_result.get("best_state")
    assigned = _extract_assigned_store_ids(best_state)
    missing_ids = [sid for sid in stores.keys() if sid not in assigned]
    if not missing_ids:
        return []

    vehicles_state = best_state if isinstance(best_state, list) else []
    wave = payload.get("wave") if isinstance(payload.get("wave"), dict) else {}
    scenario = payload.get("scenario") if isinstance(payload.get("scenario"), dict) else {}
    dist = payload.get("dist") if isinstance(payload.get("dist"), dict) else {}
    valid_store_ids = set(stores.keys())

    def sanitize_routes(routes):
        cleaned_routes = []
        seen = set()
        for route in routes if isinstance(routes, list) else []:
            if not isinstance(route, list):
                continue
            cleaned = []
            for raw_sid in route:
                sid2 = _normalize_store_id(raw_sid)
                if not sid2 or sid2 not in valid_store_ids or sid2 in seen:
                    continue
                seen.add(sid2)
                cleaned.append(sid2)
            if cleaned:
                cleaned_routes.append(cleaned)
        return cleaned_routes

    records = []
    for sid in missing_ids:
        store = stores.get(sid) or {}
        current_time = minutes_to_hhmm(_coerce_float(store.get("desiredArrivalMin")))
        latest_allowed = minutes_to_hhmm(_coerce_float(store.get("latestAllowedArrivalMin")))
        best_arrival_detail = None
        best_other_detail = None
        reason_counts = {}

        for item in vehicles_state:
            if not isinstance(item, dict):
                continue
            base_routes = sanitize_routes(item.get("routes"))
            candidate_variants = [base_routes + [[sid]]]
            for ridx, route in enumerate(base_routes):
                for pos in range(len(route) + 1):
                    new_routes = [list(r) if isinstance(r, list) else [] for r in base_routes]
                    new_routes[ridx].insert(pos, sid)
                    candidate_variants.append(new_routes)

            for routes in candidate_variants:
                probe = dict(item)
                probe["routes"] = routes
                checked = check_plan_constraints(probe, stores, dist, wave, scenario)
                if checked.get("feasible"):
                    continue
                violations = checked.get("violations") or []
                violation = violations[0] if violations else {}
                failure_type = _normalize_reason(violation.get("type"))
                reason_counts[failure_type] = int(reason_counts.get(failure_type) or 0) + 1

                if failure_type == "arrival_window":
                    detail = _simulate_candidate_timing(item, routes, stores, dist, wave, scenario, sid)
                    if detail and (
                        best_arrival_detail is None
                        or detail.get("late_minutes", 10**9) < best_arrival_detail.get("late_minutes", 10**9)
                    ):
                        best_arrival_detail = detail
                elif best_other_detail is None:
                    best_other_detail = _build_non_arrival_detail(
                        item,
                        routes,
                        stores,
                        dist,
                        wave,
                        scenario,
                        wave_id,
                        sid,
                        failure_type,
                        violation,
                    )

        detail = best_arrival_detail or best_other_detail or {
            "target_vehicle": "",
            "wave_id": _normalize_wave_key((wave or {}).get("waveId") or wave_id),
            "failure_type": max(reason_counts, key=reason_counts.get) if reason_counts else "mixed",
            "violation_type": "",
            "current_time": current_time,
            "expected_arrival": None,
            "latest_allowed": latest_allowed,
            "late_minutes": 0,
            "route_stops": [],
            "suggestion_type": "info",
            "suggestion_text": "当前失败类型暂不支持自动建议。",
        }
        detail.update(
            {
                "batch_id": str(batch_id or "").strip(),
                "wave_id": _normalize_wave_key(detail.get("wave_id") or wave_id),
                "shop_code": sid,
            }
        )
        records.append(detail)
    return records


def serialize_route_stops(route_stops):
    try:
        return json.dumps(route_stops or [], ensure_ascii=False)
    except Exception:
        return "[]"
