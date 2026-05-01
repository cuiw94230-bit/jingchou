"""目标函数模块：把业务目标转换成可比较的成本系数。"""

def _clamp_weight_0_100(value, default=50.0):
    try:
        return max(0.0, min(100.0, float(value)))
    except Exception:
        return float(default)
def _to_bool(value, default):
    if value is None:
        return bool(default)
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"true", "1", "yes", "on"}:
            return True
        if text in {"false", "0", "no", "off"}:
            return False
    return bool(value)


def resolve_cost_coefficients(strategy_config_resolved):
    coeffs = {
        "arrivalViolationPenaltyCoeff": 1.0,
        "latenessPenaltyCoeff": 1.0,
        "waveLatePenaltyCoeff": 1.0,
        "vehicleBusyPenaltyCoeff": 1.0,
        "extraTripPenaltyCoeff": 1.0,
        "lateRoutePenaltyCoeff": 1.0,
        "distanceCostCoeff": 1.0,
        "loadBonusCoeff": 1.0,
    }
    if not isinstance(strategy_config_resolved, dict):
        return coeffs

    def read_value(key, default=None, group_key=None):
        value = strategy_config_resolved.get(key)
        if value is None and group_key:
            group = strategy_config_resolved.get(group_key)
            if isinstance(group, dict):
                value = group.get(key)
        return default if value is None else value

    allow_late = _to_bool(read_value("allowLate", True, "lateness"), True)
    lateness_strength = str(read_value("latenessStrength", "medium", "lateness") or "medium").strip().lower()
    late_route_strength = str(read_value("lateRouteStrength", "medium", "lateRoute") or "medium").strip().lower()
    allow_multi_trip = _to_bool(read_value("allowMultiTrip", True, "multiTrip"), True)
    vehicle_busy_weight = _clamp_weight_0_100(read_value("vehicleBusyCostWeight", 50.0, "vehicleBusy"), 50.0)
    distance_weight = _clamp_weight_0_100(read_value("distanceWeight", 50.0), 50.0)
    load_weight = _clamp_weight_0_100(read_value("loadWeight", 50.0), 50.0)
    wave_delay_weight = _clamp_weight_0_100(read_value("waveDelayPenalty", 50.0, "waveDelay"), 50.0)

    lateness_strength_coeff = {"low": 0.75, "medium": 1.0, "high": 1.35}.get(lateness_strength, 1.0)
    arrival_strength_coeff = {"low": 0.70, "medium": 1.0, "high": 1.50}.get(lateness_strength, 1.0)
    # 默认 allowLate=True 时保持现有逻辑，关闭时显著提高迟到相关惩罚。
    allow_late_coeff = 1.0 if allow_late else 1.30
    allow_late_arrival_coeff = 1.0 if allow_late else 1.80

    coeffs["latenessPenaltyCoeff"] = lateness_strength_coeff * allow_late_coeff
    coeffs["arrivalViolationPenaltyCoeff"] = arrival_strength_coeff * allow_late_arrival_coeff
    coeffs["extraTripPenaltyCoeff"] = 1.0 if allow_multi_trip else 1.35
    coeffs["vehicleBusyPenaltyCoeff"] = 0.5 + vehicle_busy_weight / 100.0
    coeffs["distanceCostCoeff"] = 0.5 + distance_weight / 100.0
    coeffs["loadBonusCoeff"] = 0.5 + load_weight / 100.0
    coeffs["waveLatePenaltyCoeff"] = 0.5 + wave_delay_weight / 100.0
    coeffs["lateRoutePenaltyCoeff"] = {"low": 0.70, "medium": 1.0, "high": 1.40}.get(late_route_strength, 1.0)
    return coeffs


def score_plan_components(item, wave, scenario, summary):
    strategy_cfg = scenario.get("strategyConfigResolved") if isinstance(scenario, dict) else {}
    coeffs = resolve_cost_coefficients(strategy_cfg)
    vehicle_busy_penalty = (
        0
        if wave["singleWave"]
        else (item.get("priorRegularDistance", 0.0) * 1.2 + item.get("priorWaveCount", 0) * 150) * coeffs["vehicleBusyPenaltyCoeff"]
    )
    lateness_penalty = summary["latenessMinutes"] * 60 * coeffs["latenessPenaltyCoeff"]
    arrival_violation_penalty = summary["arrivalViolationMinutes"] * 20000 * coeffs["arrivalViolationPenaltyCoeff"]
    wave_late_penalty = summary["waveLateMinutes"] * 80 * coeffs["waveLatePenaltyCoeff"]
    extra_trip_count = max(0, summary["tripCount"] - 1) if not wave["singleWave"] else 0
    extra_trip_penalty = extra_trip_count * 180 * coeffs["extraTripPenaltyCoeff"]
    late_route_penalty = summary["lateRouteCount"] * (1600 if scenario["concentrateLate"] else 240) * coeffs["lateRoutePenaltyCoeff"]
    distance_cost = summary["totalDistance"] * 0.45 * coeffs["distanceCostCoeff"]
    load_bonus = summary["totalLoad"] * 0.08 * coeffs["loadBonusCoeff"]
    total_cost = (
        arrival_violation_penalty
        + lateness_penalty
        + wave_late_penalty
        + vehicle_busy_penalty
        + extra_trip_penalty
        + late_route_penalty
        + distance_cost
        - load_bonus
    )
    components = {
        "arrivalViolationPenalty": arrival_violation_penalty,
        "latenessPenalty": lateness_penalty,
        "waveLatePenalty": wave_late_penalty,
        "vehicleBusyPenalty": vehicle_busy_penalty,
        "extraTripPenalty": extra_trip_penalty,
        "lateRoutePenalty": late_route_penalty,
        "distanceCost": distance_cost,
        "loadBonus": load_bonus,
    }
    return {"cost": total_cost, "components": components}
