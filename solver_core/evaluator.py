"""方案评估模块：把路线状态转换成分数、统计项和明细结果。"""

from .constraints import check_plan_constraints
from .objective import score_plan_components
from .utils.route_wave import labelRouteWaveByDiff100


def _extract_route_ids(item):
    if not isinstance(item, dict):
        return []
    for key in ("route_ids", "routeIds", "route_id_list", "routeIdList"):
        value = item.get(key)
        if isinstance(value, (list, tuple)):
            return list(value)
    return []


def _resolve_delivery_date(scenario, wave):
    scenario = scenario or {}
    wave = wave or {}
    return (
        scenario.get("delivery_date")
        or scenario.get("deliveryDate")
        or wave.get("delivery_date")
        or wave.get("deliveryDate")
        or ""
    )


def _compute_wave_stats(state, wave, scenario):
    grouped = {}
    total_routes = 0
    delivery_date = _resolve_delivery_date(scenario, wave)

    for item in state or []:
        routes = item.get("routes", []) if isinstance(item, dict) else []
        route_count = len(routes)
        if route_count <= 0:
            continue

        total_routes += route_count
        vehicle_key = ""
        if isinstance(item, dict):
            vehicle_key = str(item.get("plateNo") or item.get("vehicle_id") or item.get("vehicleId") or "")
        group_key = (delivery_date, vehicle_key)
        bucket = grouped.setdefault(group_key, {"route_ids": [], "route_count": 0, "fallback_single": 0})
        bucket["route_count"] += route_count

        route_ids = _extract_route_ids(item)
        if route_ids:
            bucket["route_ids"].extend(route_ids)
        else:
            bucket["fallback_single"] += route_count

    first_count = 0
    second_count = 0
    single_count = 0

    for bucket in grouped.values():
        route_ids = bucket["route_ids"]
        route_count = int(bucket["route_count"])
        fallback_single = int(bucket["fallback_single"])
        labels = labelRouteWaveByDiff100(route_ids) if route_ids else {}

        local_first = sum(1 for value in labels.values() if value == "first")
        local_second = sum(1 for value in labels.values() if value == "second")
        local_single = sum(1 for value in labels.values() if value == "single")

        covered = local_first + local_second + local_single
        uncovered = max(0, route_count - covered)

        first_count += local_first
        second_count += local_second
        single_count += local_single + fallback_single + uncovered

    return {
        "total_routes": int(total_routes),
        "first_count": int(first_count),
        "second_count": int(second_count),
        "single_count": int(single_count),
    }


def _resolve_dual_wave_config(scenario):
    cfg = scenario.get("strategyConfigResolved") if isinstance(scenario, dict) else {}
    if not isinstance(cfg, dict):
        return {"active": False, "coeff": 0.0}
    active = bool(cfg.get("dualWaveActive"))
    try:
        coeff = float(cfg.get("dualWaveCoeff", 0.0))
    except Exception:
        coeff = 0.0
    if coeff < 0:
        coeff = 0.0
    return {"active": active, "coeff": coeff}


def _compute_dual_wave_penalty(wave_stats, scenario):
    config = _resolve_dual_wave_config(scenario)
    if not config.get("active"):
        return 0.0
    if not isinstance(wave_stats, dict):
        return 0.0
    first_count = int(wave_stats.get("first_count") or 0)
    second_count = int(wave_stats.get("second_count") or 0)
    single_count = int(wave_stats.get("single_count") or 0)
    unmatched_pairs = abs(first_count - second_count)
    penalty_units = max(0, single_count) + max(0, unmatched_pairs)
    return float(penalty_units) * 240.0 * float(config.get("coeff", 0.0))


def evaluate_plan(item, stores, dist, wave, scenario):
    checked = check_plan_constraints(item, stores, dist, wave, scenario)
    if not checked.get("feasible"):
        return {
            "feasible": False,
            "cost": 0.0,
            "components": {},
            "violations": checked.get("violations") or [],
            "totalDistance": 0.0,
            "totalLoad": 0.0,
            "tripCount": 0,
        }
    summary = checked["summary"]
    scored = score_plan_components(item, wave, scenario, summary)
    return {
        "feasible": True,
        "cost": scored["cost"],
        "components": scored["components"],
        "violations": checked.get("violations") or [],
        "totalDistance": summary["totalDistance"],
        "totalLoad": summary["totalLoad"],
        "tripCount": summary["tripCount"],
    }


def evaluate_state(state, stores, dist, wave, scenario):
    total_cost = 0.0
    for item in state:
        plan = evaluate_plan(item, stores, dist, wave, scenario)
        if not plan.get("feasible"):
            return None
        total_cost += plan["cost"]
    wave_stats = _compute_wave_stats(state, wave, scenario)
    total_cost += _compute_dual_wave_penalty(wave_stats, scenario)
    return total_cost


def evaluate_state_details(state, stores, dist, wave, scenario):
    total_cost = 0.0
    totals = {
        "arrivalViolationPenalty": 0.0,
        "latenessPenalty": 0.0,
        "waveLatePenalty": 0.0,
        "vehicleBusyPenalty": 0.0,
        "extraTripPenalty": 0.0,
        "lateRoutePenalty": 0.0,
        "distanceCost": 0.0,
        "loadBonus": 0.0,
        "dualWavePenalty": 0.0,
    }
    for item in state:
        plan = evaluate_plan(item, stores, dist, wave, scenario)
        if not plan.get("feasible"):
            return None
        total_cost += float(plan["cost"])
        components = plan.get("components") or {}
        for key in totals:
            totals[key] += float(components.get(key) or 0.0)
    wave_stats = _compute_wave_stats(state, wave, scenario)
    dual_wave_penalty = _compute_dual_wave_penalty(wave_stats, scenario)
    totals["dualWavePenalty"] = float(dual_wave_penalty)
    total_cost += float(dual_wave_penalty)
    return {
        "cost": total_cost,
        "components": totals,
        "wave_stats": wave_stats,
    }