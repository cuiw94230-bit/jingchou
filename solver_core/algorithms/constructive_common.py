"""构造式算法公共逻辑：初始解生成、缺店回填和构造式求解公共能力。"""

import time

from ..common_ops import (    clone_state,
    empty_like_state,
    greedy_insert_missing,
    normalize_routes,
    savings_insert_missing,
    score_for_savings_order,
    vrptw_insert_missing,
)
from ..evaluator import evaluate_state, evaluate_state_details


def solve_constructive(payload, algorithm_key):
    started_at = time.perf_counter()
    scenario = payload["scenario"]
    wave = payload["wave"]
    stores = {item["id"]: item for item in payload["stores"]}
    dist = payload["dist"]
    base_state = normalize_routes(clone_state(payload["vehicles"]))
    all_store_ids = sorted(stores.keys(), key=lambda sid: str(sid))
    if not all_store_ids:
        return {"bestState": base_state, "traceLog": []}

    trace_log = []

    def _format_savings_diagnostic(diag, wave_id):
        if not isinstance(diag, dict):
            return ""
        step_no = int(diag.get("stepNo") or 0)
        store_id = str(diag.get("storeId") or "")
        store_name = str(diag.get("storeName") or "")
        boxes = float(diag.get("boxes") or 0.0)
        candidate_vehicle_count = int(diag.get("candidateVehicleCount") or 0)
        vehicle_limit = int(diag.get("vehicleLimit") or 0)
        lines = [
            f"[WAVE={wave_id}] [ALG=savings] [STEP={step_no}] [STORE={store_id}] [STORE_NAME={store_name}] [BOXES={boxes:.4f}]",
            f"候选车辆数={candidate_vehicle_count}，评估上限={vehicle_limit}",
        ]
        for row in (diag.get("candidates") or []):
            plate = str(row.get("plateNo") or "-")
            rank = int(row.get("rank") or 0)
            route_count = int(row.get("routeCount") or 0)
            trip_no = row.get("tripNo")
            pos = int(row.get("pos") or 0)
            delta_km = float(row.get("deltaKm") or 0.0)
            feasible = bool(row.get("feasible"))
            lines.append(f"- 车辆={plate} 候选序号={rank} 现有趟数={route_count}")
            lines.append(f"  最佳插入：trip={trip_no} pos={pos} delta_km={delta_km:.2f}")
            if feasible:
                lines.append(
                    "  试算目标：distance={distance} extraTrip={extra} busy={busy} lateSoft={late} loadBonus={load} objective={obj}".format(
                        distance=float(row.get("distanceCost") or 0.0),
                        extra=float(row.get("extraTripPenalty") or 0.0),
                        busy=float(row.get("vehicleBusyPenalty") or 0.0),
                        late=float(row.get("lateSoftPenalty") or 0.0),
                        load=float(row.get("loadBonus") or 0.0),
                        obj=float(row.get("objective") or 0.0),
                    )
                )
                lines.append("  约束结果：feasible")
            else:
                lines.append("  约束结果：infeasible")
                lines.append(f"  失败原因：{str(row.get('reasonCode') or 'no_feasible_candidate')}")
                lines.append(f"  失败明细：{str(row.get('reasonDetail') or '-')}")
        chosen = diag.get("chosen") if isinstance(diag.get("chosen"), dict) else None
        if chosen:
            lines.append(
                f"最终选择：车辆={str(chosen.get('plateNo') or '-')}"
                f" trip={str(chosen.get('tripNo') or '-')}"
                f" pos={int(chosen.get('pos') or 0)}"
                f" objective={float(chosen.get('objective') or 0.0):.2f}"
            )
        else:
            lines.append("最终选择：无")
        lines.append(f"步骤结果：{str(diag.get('result') or 'rejected')}")
        return "\n".join(lines)

    if algorithm_key == "vrptw":
        ordered_ids = sorted(
            all_store_ids,
            key=lambda sid: (
                float((stores.get(sid) or {}).get("desiredArrivalMin", 10**9)),
                float((stores.get(sid) or {}).get("latestAllowedArrivalMin", 10**9)),
                str(sid),
            ),
        )
    else:
        ordered_ids = sorted(
            all_store_ids,
            key=lambda sid: (
                -score_for_savings_order(sid, all_store_ids, dist),
                float((stores.get(sid) or {}).get("desiredArrivalMin", 10**9)),
                str(sid),
            ),
        )

    current = empty_like_state(base_state)
    best_cost = None
    trace_log.append(
        {
            "algorithmKey": algorithm_key,
            "scope": "wave",
            "waveId": wave["waveId"],
            "stage": f"{algorithm_key}-python-start",
            "textZh": "",
            "textJa": "",
        }
    )

    for index, store_id in enumerate(ordered_ids):
        if algorithm_key == "vrptw":
            inserted = vrptw_insert_missing(
                current,
                [store_id],
                stores,
                dist,
                wave,
                scenario,
                vehicle_limit=max(1, len(current)),
            )
        else:
            diag_rows = []
            inserted = savings_insert_missing(
                current,
                [store_id],
                stores,
                dist,
                wave,
                scenario,
                vehicle_limit=max(1, len(current)),
                diagnostics=diag_rows,
            )
            if diag_rows:
                trace_log.append(
                    {
                        "algorithmKey": algorithm_key,
                        "scope": "wave",
                        "waveId": wave["waveId"],
                        "stage": f"{algorithm_key}-python-iteration",
                        "iteration": index,
                        "storeId": store_id,
                        "textZh": _format_savings_diagnostic(diag_rows[-1], wave["waveId"]),
                        "textJa": "",
                        "candidates": diag_rows[-1].get("candidates") or [],
                    }
                )
        if not inserted:
            continue
        current = normalize_routes(inserted)
        evaluated = evaluate_state_details(current, stores, dist, wave, scenario)
        if not evaluated:
            continue
        best_cost = float(evaluated["cost"])
        if algorithm_key != "savings":
            trace_log.append(
                {
                    "algorithmKey": algorithm_key,
                    "scope": "wave",
                    "waveId": wave["waveId"],
                    "stage": f"{algorithm_key}-python-iteration",
                    "iteration": index,
                    "bestCost": round(best_cost, 2),
                    "costBreakdown": {
                        k: round(float(v or 0.0), 2) for k, v in ((evaluated.get("components") or {}).items())
                    },
                    "textZh": "",
                    "textJa": "",
                }
            )

    _ = (time.perf_counter() - started_at) * 1000.0
    if best_cost is None:
        best_cost = (
            evaluate_state(current, stores, dist, wave, scenario)
            or evaluate_state(base_state, stores, dist, wave, scenario)
            or 0.0
        )
    final_eval = evaluate_state_details(current, stores, dist, wave, scenario) or {"components": {}}
    trace_log.append(
        {
            "algorithmKey": algorithm_key,
            "scope": "wave",
            "waveId": wave["waveId"],
            "stage": f"{algorithm_key}-python-finish",
            "bestCost": round(float(best_cost), 2),
            "costBreakdown": {
                k: round(float(v or 0.0), 2) for k, v in ((final_eval.get("components") or {}).items())
            },
            "textZh": "",
            "textJa": "",
        }
    )
    return {"bestState": current, "traceLog": trace_log}


def build_initial_from_order(store_order, vehicles, stores, dist, wave, scenario):
    state = empty_like_state(vehicles)
    for store_id in store_order:
        if store_id not in stores:
            continue
        inserted = greedy_insert_missing(state, [store_id], stores, dist, wave, scenario, vehicle_limit=len(vehicles))
        if inserted:
            state = normalize_routes(inserted)
    return state


def generate_diverse_initial_state(payload, algorithm_key, rng):
    scenario = payload["scenario"]
    wave = payload["wave"]
    stores = {item["id"]: item for item in payload["stores"]}
    vehicles = payload["vehicles"]
    dist = payload["dist"]

    base_state = normalize_routes(clone_state(vehicles))
    all_store_ids = sorted(stores.keys(), key=lambda sid: str(sid))
    if not all_store_ids:
        return base_state

    if algorithm_key in ("sa", "tabu"):
        shuffled_ids = list(all_store_ids)
        rng.shuffle(shuffled_ids)
        return build_initial_from_order(shuffled_ids, vehicles, stores, dist, wave, scenario)
    if algorithm_key == "lns":
        sorted_ids = sorted(all_store_ids, key=lambda sid: dist["DC"].get(sid, 9999))
        return build_initial_from_order(sorted_ids, vehicles, stores, dist, wave, scenario)
    if algorithm_key == "aco":
        sorted_ids = sorted(
            all_store_ids,
            key=lambda sid: (
                stores[sid].get("desiredArrivalMin", 0),
                stores[sid].get("latestAllowedArrivalMin", 9999),
            ),
        )
        return build_initial_from_order(sorted_ids, vehicles, stores, dist, wave, scenario)
    if algorithm_key == "pso":
        shuffled_ids = list(all_store_ids)
        rng.shuffle(shuffled_ids)
        return build_initial_from_order(shuffled_ids, vehicles, stores, dist, wave, scenario)
    if algorithm_key == "hybrid":
        constructive_payload = {
            "scenario": scenario,
            "wave": wave,
            "stores": payload["stores"],
            "vehicles": vehicles,
            "dist": dist,
        }
        constructive_result = solve_constructive(constructive_payload, "savings")
        return constructive_result["bestState"]

    constructive_payload = {
        "scenario": scenario,
        "wave": wave,
        "stores": payload["stores"],
        "vehicles": vehicles,
        "dist": dist,
    }
    constructive_result = solve_constructive(constructive_payload, "savings")
    return constructive_result["bestState"]
