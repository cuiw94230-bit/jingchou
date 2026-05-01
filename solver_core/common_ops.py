"""求解器公共操作库：state 拷贝、哈希、插入搜索与缺店回填都在这里。"""

import math

DC_ID = "DC"


def _dist_value(dist, from_id, to_id, default=9999.0):
    return float((dist.get(from_id, {}) or {}).get(to_id, default))

def clone_state(state):
    return [
        {
            "plateNo": item["plateNo"],
            "capacity": item["capacity"],
            "speed": item["speed"],
            "canCarryCold": item["canCarryCold"],
            "priorRegularDistance": item.get("priorRegularDistance", 0.0),
            "priorWaveCount": item.get("priorWaveCount", 0),
            "earliestDepartureMin": item.get("earliestDepartureMin", 0),
            "routes": [list(route) for route in item.get("routes", []) if route],
        }
        for item in state
    ]


def hash_state(state):
    return "||".join(
        f"{item['plateNo']}:{'|'.join('>'.join(route) for route in item['routes'])}"
        for item in state
    )


def normalize_routes(state):
    for item in state:
        item["routes"] = [route for route in item.get("routes", []) if route]
    return state


def route_distance(route, dist):
    if not route:
        return 0.0
    total = _dist_value(dist, DC_ID, route[0])
    for index in range(len(route) - 1):
        total += _dist_value(dist, route[index], route[index + 1])
    total += _dist_value(dist, route[-1], DC_ID)
    return total


def best_insert_for_vehicle(state, vehicle_index, store_id, dist):
    item = state[vehicle_index]
    best_routes = None
    best_delta = math.inf
    routes = item.get("routes", [])
    for route_index, route in enumerate(routes):
        for position in range(len(route) + 1):
            prev_id = DC_ID if position == 0 else route[position - 1]
            next_id = DC_ID if position == len(route) else route[position]
            delta = _dist_value(dist, prev_id, store_id) + _dist_value(dist, store_id, next_id) - _dist_value(dist, prev_id, next_id)
            if delta < best_delta:
                candidate_routes = [list(r) for r in routes]
                candidate_routes[route_index] = route[:position] + [store_id] + route[position:]
                best_delta = delta
                best_routes = candidate_routes
    empty_trip_delta = _dist_value(dist, DC_ID, store_id) + _dist_value(dist, store_id, DC_ID)
    if empty_trip_delta < best_delta:
        best_routes = [list(r) for r in routes] + [[store_id]]
        best_delta = empty_trip_delta
    return best_routes, best_delta


def _describe_insert_position(original_routes, candidate_routes, store_id):
    before = [list(r) for r in (original_routes or [])]
    after = [list(r) for r in (candidate_routes or [])]
    if len(after) > len(before):
        route = after[-1]
        if route and route[0] == store_id:
            return {"tripNo": "new", "pos": 0}
    for idx, route in enumerate(after):
        if store_id not in route:
            continue
        pos = route.index(store_id)
        if idx >= len(before):
            return {"tripNo": idx + 1, "pos": pos}
        b = before[idx]
        if len(route) != len(b):
            return {"tripNo": idx + 1, "pos": pos}
        for j in range(min(len(route), len(b))):
            if route[j] != b[j]:
                return {"tripNo": idx + 1, "pos": pos}
    return {"tripNo": "-", "pos": -1}


def greedy_insert_missing(state, missing, stores, dist, wave, scenario, vehicle_limit=6):
    from .evaluator import evaluate_state

    current = clone_state(state)
    for store_id in missing:
        ranked = []
        for vehicle_index, item in enumerate(current):
            routes, delta = best_insert_for_vehicle(current, vehicle_index, store_id, dist)
            ranked.append((delta + len(item.get("routes", [])) * 1.5, vehicle_index, routes))
        ranked.sort(key=lambda item: item[0])
        best_state = None
        best_cost = math.inf
        for _, vehicle_index, candidate_routes in ranked[: max(1, min(vehicle_limit, len(ranked)) )]:
            candidate = clone_state(current)
            candidate[vehicle_index]["routes"] = candidate_routes
            candidate_cost = evaluate_state(candidate, stores, dist, wave, scenario)
            if candidate_cost is not None and candidate_cost < best_cost:
                best_cost = candidate_cost
                best_state = candidate
        if best_state is None:
            return None
        current = normalize_routes(best_state)
    return current


def vrptw_insert_missing(state, missing, stores, dist, wave, scenario, vehicle_limit=6):
    from .evaluator import evaluate_state_details

    current = clone_state(state)
    for store_id in missing:
        ranked = []
        for vehicle_index, item in enumerate(current):
            routes, delta = best_insert_for_vehicle(current, vehicle_index, store_id, dist)
            ranked.append((delta + len(item.get("routes", [])) * 1.2, vehicle_index, routes))
        ranked.sort(key=lambda item: item[0])
        best_state = None
        best_tuple = None
        for _, vehicle_index, candidate_routes in ranked[: max(1, min(vehicle_limit, len(ranked)) )]:
            candidate = clone_state(current)
            candidate[vehicle_index]["routes"] = candidate_routes
            details = evaluate_state_details(candidate, stores, dist, wave, scenario)
            if not details:
                continue
            parts = details.get("components") or {}
            lateness = float(parts.get("arrivalViolationPenalty", 0.0)) + float(parts.get("latenessPenalty", 0.0)) + float(parts.get("waveLatePenalty", 0.0))
            distance = float(parts.get("distanceCost", 0.0))
            extra = float(parts.get("extraTripPenalty", 0.0)) + float(parts.get("vehicleBusyPenalty", 0.0)) + float(parts.get("lateRoutePenalty", 0.0))
            objective = lateness * 1.0 + distance * 0.22 + extra * 0.15 - float(parts.get("loadBonus", 0.0)) * 0.05
            tie = float(details.get("cost", objective))
            score_tuple = (objective, tie)
            if best_tuple is None or score_tuple < best_tuple:
                best_tuple = score_tuple
                best_state = candidate
        if best_state is None:
            return None
        current = normalize_routes(best_state)
    return current


def savings_insert_missing(state, missing, stores, dist, wave, scenario, vehicle_limit=6, diagnostics=None):
    from .evaluator import evaluate_state_details, evaluate_plan

    current = clone_state(state)
    for step_index, store_id in enumerate(missing):
        ranked = []
        for vehicle_index, item in enumerate(current):
            routes, delta = best_insert_for_vehicle(current, vehicle_index, store_id, dist)
            ranked.append((delta + len(item.get("routes", [])) * 0.3, vehicle_index, routes, delta))
        ranked.sort(key=lambda item: item[0])
        best_state = None
        best_tuple = None
        candidate_rows = []
        candidate_scope = ranked[: max(1, min(vehicle_limit, len(ranked)) )]
        for rank_no, (_, vehicle_index, candidate_routes, base_delta) in enumerate(candidate_scope, start=1):
            vehicle = current[vehicle_index]
            candidate = clone_state(current)
            candidate[vehicle_index]["routes"] = candidate_routes
            details = evaluate_state_details(candidate, stores, dist, wave, scenario)
            if not details:
                plan_eval = evaluate_plan(candidate[vehicle_index], stores, dist, wave, scenario)
                violations = plan_eval.get("violations") if isinstance(plan_eval, dict) else []
                first_violation = violations[0] if violations else {}
                reason_code = str(first_violation.get("type") or "no_feasible_candidate")
                reason_detail = str(first_violation) if first_violation else "candidate_not_feasible"
                insert_pos = _describe_insert_position(vehicle.get("routes", []), candidate_routes, store_id)
                candidate_rows.append(
                    {
                        "rank": rank_no,
                        "plateNo": vehicle.get("plateNo"),
                        "routeCount": len(vehicle.get("routes", [])),
                        "tripNo": insert_pos.get("tripNo"),
                        "pos": insert_pos.get("pos"),
                        "deltaKm": round(float(base_delta), 2),
                        "feasible": False,
                        "reasonCode": reason_code,
                        "reasonDetail": reason_detail,
                    }
                )
                continue
            parts = details.get("components") or {}
            distance = float(parts.get("distanceCost", 0.0))
            trip_penalty = float(parts.get("extraTripPenalty", 0.0))
            busy = float(parts.get("vehicleBusyPenalty", 0.0))
            lateness_soft = float(parts.get("arrivalViolationPenalty", 0.0)) + float(parts.get("latenessPenalty", 0.0)) + float(parts.get("waveLatePenalty", 0.0))
            objective = distance * 1.0 + trip_penalty * 0.35 + busy * 0.10 + lateness_soft * 0.08 - float(parts.get("loadBonus", 0.0)) * 0.08
            tie = float(details.get("cost", objective))
            score_tuple = (objective, tie)
            insert_pos = _describe_insert_position(vehicle.get("routes", []), candidate_routes, store_id)
            candidate_rows.append(
                {
                    "rank": rank_no,
                    "plateNo": vehicle.get("plateNo"),
                    "routeCount": len(vehicle.get("routes", [])),
                    "tripNo": insert_pos.get("tripNo"),
                    "pos": insert_pos.get("pos"),
                    "deltaKm": round(float(base_delta), 2),
                    "feasible": True,
                    "reasonCode": "",
                    "reasonDetail": "",
                    "distanceCost": round(distance, 2),
                    "extraTripPenalty": round(trip_penalty, 2),
                    "vehicleBusyPenalty": round(busy, 2),
                    "lateSoftPenalty": round(lateness_soft, 2),
                    "loadBonus": round(float(parts.get("loadBonus", 0.0)), 2),
                    "objective": round(objective, 2),
                    "tieCost": round(tie, 2),
                }
            )
            if best_tuple is None or score_tuple < best_tuple:
                best_tuple = score_tuple
                best_state = candidate
                chosen_meta = {
                    "plateNo": vehicle.get("plateNo"),
                    "tripNo": insert_pos.get("tripNo"),
                    "pos": insert_pos.get("pos"),
                    "objective": round(objective, 2),
                }
        if isinstance(diagnostics, list):
            store_meta = stores.get(store_id) or {}
            diagnostics.append(
                {
                    "stepNo": step_index + 1,
                    "storeId": store_id,
                    "storeName": str(store_meta.get("name") or ""),
                    "boxes": round(float(store_meta.get("boxes") or 0.0), 4),
                    "candidateVehicleCount": len(ranked),
                    "vehicleLimit": max(1, min(vehicle_limit, len(ranked))),
                    "candidates": candidate_rows,
                    "result": "inserted" if best_state is not None else "rejected",
                    "chosen": chosen_meta if best_state is not None else None,
                }
            )
        if best_state is None:
            return None
        current = normalize_routes(best_state)
    return current


def empty_like_state(state):
    return [
        {
            "plateNo": item["plateNo"],
            "capacity": item["capacity"],
            "speed": item["speed"],
            "canCarryCold": item.get("canCarryCold", False),
            "priorRegularDistance": item.get("priorRegularDistance", 0.0),
            "priorWaveCount": item.get("priorWaveCount", 0),
            "earliestDepartureMin": item.get("earliestDepartureMin", 0),
            "routes": [],
        }
        for item in state
    ]


def collect_unique_stores(state):
    seen = set()
    ordered = []
    for item in state:
        for route in item.get("routes", []):
            for store_id in route:
                if store_id in seen:
                    continue
                seen.add(store_id)
                ordered.append(store_id)
    return ordered


def score_for_savings_order(store_id, all_ids, dist):
    best_saving = -1e18
    for other_id in all_ids:
        if other_id == store_id:
            continue
        saving = _dist_value(dist, DC_ID, store_id) + _dist_value(dist, DC_ID, other_id) - _dist_value(dist, store_id, other_id)
        if saving > best_saving:
            best_saving = saving
    return best_saving
