"""约束校验模块：统一检查时间窗、容量、里程、波次结束时刻等约束。"""

from .common_ops import DC_ID

IGNORE_CAPACITY_CONSTRAINT = False
def _to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _to_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _to_bool(value, default=False):
    if value is None:
        return bool(default)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
    return bool(value)


def _resolve_difficulty_config(scenario):
    cfg = (scenario or {}).get("strategyConfigResolved") if isinstance(scenario, dict) else {}
    if not isinstance(cfg, dict):
        cfg = {}
    mode = str(cfg.get("deliveryDifficultyMode") or "time").strip().lower()
    if mode not in {"time", "count", "score"}:
        mode = "time"
    tier1_unlimited = _to_bool(cfg.get("difficultyTier1Unlimited"), False)
    tier2_unlimited = _to_bool(cfg.get("difficultyTier2Unlimited"), False)
    tier3_unlimited = _to_bool(cfg.get("difficultyTier3Unlimited"), True)
    tier1_limit = max(0, _to_int(cfg.get("difficultyTier1Limit"), 1))
    tier2_limit = max(0, _to_int(cfg.get("difficultyTier2Limit"), 2))
    tier3_limit = max(0, _to_int(cfg.get("difficultyTier3Limit"), 0))
    # 兼容旧请求: 仅当前台未传分档字段时作为兜底。
    legacy = cfg.get("difficultyCountLimitLegacy")
    if legacy is None:
        legacy = cfg.get("difficultyCountLimit")
    if legacy is not None and all(k not in cfg for k in (
        "difficultyTier1Unlimited",
        "difficultyTier1Limit",
        "difficultyTier2Unlimited",
        "difficultyTier2Limit",
        "difficultyTier3Unlimited",
        "difficultyTier3Limit",
    )):
        tier1_unlimited = False
        tier1_limit = max(0, _to_int(legacy, 3))
        tier2_unlimited = True
        tier3_unlimited = True
    return {
        "mode": mode,
        "tier1_unlimited": tier1_unlimited,
        "tier1_limit": tier1_limit,
        "tier2_unlimited": tier2_unlimited,
        "tier2_limit": tier2_limit,
        "tier3_unlimited": tier3_unlimited,
        "tier3_limit": tier3_limit,
        "score_limit": max(1.0, _to_float(cfg.get("difficultyScoreLimit"), 8.0)),
    }


def _allow_late_enabled(scenario):
    cfg = (scenario or {}).get("strategyConfigResolved") if isinstance(scenario, dict) else {}
    if not isinstance(cfg, dict):
        return True
    return _to_bool(cfg.get("allowLate"), True)


def _resolve_operational_limits(scenario, wave):
    cfg = (scenario or {}).get("strategyConfigResolved") if isinstance(scenario, dict) else {}
    if not isinstance(cfg, dict):
        cfg = {}
    # 新业务口径：不再启用全局里程上限，仅保留夜间接力上限和W3单程上限。
    max_route_km = 0.0
    relay_max_km = _to_float(cfg.get("w1w2RelayMaxKm"), 240.0)
    if relay_max_km <= 0:
        relay_max_km = 240.0
    w3_oneway_max_km = _to_float(cfg.get("w3OneWayMaxKm"), 260.0)
    if w3_oneway_max_km <= 0:
        w3_oneway_max_km = 260.0
    max_solve_capacity = _to_float(cfg.get("maxSolveCapacity"), 1.0)
    if max_solve_capacity <= 0:
        max_solve_capacity = 1.0
    wave_id = str((wave or {}).get("waveId") or "").strip().upper()
    return {
        "wave_id": wave_id,
        "max_route_km": max_route_km,
        "relay_max_km": relay_max_km,
        "w3_oneway_max_km": w3_oneway_max_km,
        "max_solve_capacity": max_solve_capacity,
    }


def resolve_store_difficulty_tier(store):
    difficulty = _to_float((store or {}).get("difficulty"), 1.0)
    if difficulty >= 1.6:
        return "tier1"
    if difficulty >= 1.3:
        return "tier2"
    return "tier3"


def check_plan_constraints(item, stores, dist, wave, scenario):
    difficulty_cfg = _resolve_difficulty_config(scenario)
    allow_late = _allow_late_enabled(scenario)
    op_limits = _resolve_operational_limits(scenario, wave)
    capacity_limit = _to_float(item.get("capacity"), _to_float(item.get("solveCapacity"), op_limits["max_solve_capacity"]))
    if capacity_limit <= 0:
        capacity_limit = op_limits["max_solve_capacity"]
    # 最早发车口径：以波次开始时间为基准，不再受全局 dispatchStartMin 卡口影响
    start_min = max(
        wave["startMin"],
        item.get("earliestDepartureMin", wave["startMin"]),
    )
    available_time = start_min
    total_distance = 0.0
    total_load = 0.0
    trip_count = 0
    lateness_minutes = 0.0
    arrival_violation_minutes = 0.0
    wave_late_minutes = 0.0
    late_route_count = 0
    violations = []

    for route_index, route in enumerate(item.get("routes", [])):
        if not route:
            continue
        current_node = DC_ID
        current_time = available_time
        load_boxes = 0.0
        trip_distance = 0.0
        outbound_distance = 0.0
        route_violation = 0.0
        route_difficulty_score = 0.0
        route_tier_counts = {"tier1": 0, "tier2": 0, "tier3": 0}
        route_first_arrival = None
        route_first_store_id = None

        for stop_index, store_id in enumerate(route):
            store = stores.get(store_id)
            if not store:
                violations.append({"type": "store_missing", "storeId": store_id, "routeIndex": route_index, "stopIndex": stop_index})
                return {"feasible": False, "violations": violations}
            # 冷链约束已停用：当前业务口径为所有店可由现有车辆承运，不再按 coldRatio/canCarryCold 拦截
            store_boxes = float(store["boxes"])
            load_boxes += store_boxes
            if (not IGNORE_CAPACITY_CONSTRAINT) and load_boxes > float(capacity_limit):
                violations.append({"type": "capacity", "storeId": store_id, "routeIndex": route_index, "stopIndex": stop_index})
                return {"feasible": False, "violations": violations}

            leg_distance = float(dist[current_node][store_id])
            arrival = current_time + leg_distance / max(float(item["speed"]), 1.0) * 60.0
            if difficulty_cfg["mode"] == "time":
                service_minutes = _to_float(store.get("actualServiceMinutes"), _to_float(store.get("serviceMinutes"), 0.0))
            else:
                service_minutes = _to_float(store.get("serviceMinutes"), _to_float(store.get("actualServiceMinutes"), 0.0))
            leave = arrival + service_minutes
            if route_first_arrival is None:
                route_first_arrival = arrival
                route_first_store_id = store_id
            trip_distance += leg_distance
            if op_limits["wave_id"] == "W3":
                outbound_distance += leg_distance
                if outbound_distance > op_limits["w3_oneway_max_km"]:
                    violations.append({"type": "max_route_km_single", "routeIndex": route_index, "stopIndex": stop_index})
                    return {"feasible": False, "violations": violations}

            desired_arrival_min = float(store["desiredArrivalMin"])
            latest_allowed_arrival_min = float(store["latestAllowedArrivalMin"])
            # 防御式跨日对齐：若上游未对齐到调度起点，则在夜波次按 +24h 修正。
            if desired_arrival_min < start_min:
                desired_arrival_min += 1440.0
                latest_allowed_arrival_min += 1440.0
            lateness_minutes += max(0.0, arrival - desired_arrival_min)
            over_tolerance = max(0.0, arrival - latest_allowed_arrival_min)
            arrival_violation_minutes += over_tolerance
            route_violation += over_tolerance
            if over_tolerance > 0 and not allow_late:
                violations.append({"type": "arrival_window", "storeId": store_id, "routeIndex": route_index, "stopIndex": stop_index})
                return {"feasible": False, "violations": violations}

            store_difficulty = _to_float(store.get("difficulty"), 1.0)
            route_difficulty_score += store_difficulty
            difficulty_tier = resolve_store_difficulty_tier(store)
            route_tier_counts[difficulty_tier] += 1

            if difficulty_cfg["mode"] == "count":
                tier_rules = (
                    ("tier1", "difficultyTier1", difficulty_cfg["tier1_unlimited"], difficulty_cfg["tier1_limit"]),
                    ("tier2", "difficultyTier2", difficulty_cfg["tier2_unlimited"], difficulty_cfg["tier2_limit"]),
                    ("tier3", "difficultyTier3", difficulty_cfg["tier3_unlimited"], difficulty_cfg["tier3_limit"]),
                )
                for tier_key, tier_label, unlimited, limit in tier_rules:
                    if unlimited:
                        continue
                    actual_count = route_tier_counts[tier_key]
                    if actual_count > limit:
                        violations.append(
                            {
                                "type": f"difficulty_{tier_key}_limit",
                                "tier": tier_label,
                                "routeIndex": route_index,
                                "stopIndex": stop_index,
                                "actualCount": actual_count,
                                "limit": limit,
                            }
                        )
                        return {"feasible": False, "violations": violations}
            if difficulty_cfg["mode"] == "score" and route_difficulty_score > difficulty_cfg["score_limit"]:
                violations.append(
                    {
                        "type": "difficulty_score_limit",
                        "routeIndex": route_index,
                        "stopIndex": stop_index,
                        "actualScore": route_difficulty_score,
                        "limit": difficulty_cfg["score_limit"],
                    }
                )
                return {"feasible": False, "violations": violations}

            current_node = store_id
            current_time = leave

        back_distance = float(dist[current_node][DC_ID])
        trip_distance += back_distance

        finish = current_time + back_distance / max(float(item["speed"]), 1.0) * 60.0
        service_end = current_time
        # 跨天波次（如 W2 21:00-07:00）统一到同一绝对分钟轴比较，避免 01:38 被误判超时。
        wave_end_min = float(wave["endMin"])
        compare_end_min = wave_end_min
        if wave_end_min < start_min:
            compare_end_min = wave_end_min + 1440.0
        compare_actual = service_end if wave["endMode"] != "return" else finish
        if wave_end_min < start_min and compare_actual < start_min:
            compare_actual += 1440.0
        route_wave_late = max(0.0, compare_actual - compare_end_min)
        if not wave["relaxEnd"] and route_wave_late > 0:
            violations.append(
                {
                    "type": "wave_end",
                    "routeIndex": route_index,
                    "startMin": start_min,
                    "waveEndMin": compare_end_min,
                    "endMode": wave["endMode"],
                    "firstStoreId": route_first_store_id,
                    "firstArrivalMin": route_first_arrival,
                    "serviceEndMin": service_end,
                    "finishMin": finish,
                    "waveLateMinutes": route_wave_late,
                }
            )
            return {"feasible": False, "violations": violations}

        total_distance += trip_distance
        total_load += load_boxes
        trip_count += 1
        wave_late_minutes += route_wave_late
        if route_violation > 0:
            late_route_count += 1
        available_time = finish

    if wave["isNightWave"] and item.get("priorRegularDistance", 0.0) + total_distance > op_limits["relay_max_km"]:
        violations.append({"type": "night_regular_distance"})
        return {"feasible": False, "violations": violations}

    return {
        "feasible": True,
        "violations": violations,
        "summary": {
            "totalDistance": total_distance,
            "totalLoad": total_load,
            "tripCount": trip_count,
            "latenessMinutes": lateness_minutes,
            "arrivalViolationMinutes": arrival_violation_minutes,
            "waveLateMinutes": wave_late_minutes,
            "lateRouteCount": late_route_count,
        },
    }
