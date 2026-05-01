"""遗传算法实现。"""
import time
import random

from ..common_ops import clone_state, hash_state, normalize_routes, greedy_insert_missing
from ..evaluator import evaluate_state, evaluate_state_details

def solve_ga(payload):
    started_at = time.perf_counter()
    rng = random.Random(payload.get("randomSeed", 203))
    compare_mode = bool(payload.get("compareMode"))
    scenario = payload["scenario"]
    wave = payload["wave"]
    stores = {item["id"]: item for item in payload["stores"]}
    vehicles = payload["vehicles"]
    dist = payload["dist"]
    initial_state = normalize_routes(clone_state(vehicles))
    population_size = 8 if compare_mode else 12
    generations = 8 if compare_mode else 12
    elite_count = 2 if compare_mode else 3
    stagnation_limit = 3 if compare_mode else 5
    improvement_threshold = 0.2 if compare_mode else 0.1
    trace_log = []
    cache = {}

    def cached_cost(state):
        key = hash_state(state)
        if key in cache:
            return cache[key]
        value = evaluate_state(state, stores, dist, wave, scenario)
        cache[key] = value
        return value

    def make_individual(state):
        cost = cached_cost(state)
        if cost is None:
            return None
        return {"state": clone_state(state), "cost": cost}

    def mutate(state, rng):
        current = clone_state(state)
        positions = []
        for v_idx, item in enumerate(current):
            for r_idx, route in enumerate(item.get("routes", [])):
                for s_idx, store in enumerate(route):
                    positions.append((v_idx, r_idx, s_idx, store))
        if not positions:
            return current
        roll = rng.random()
        if roll < 0.45 and len(positions) >= 1:
            src_v, src_r, src_s, store_id = positions[rng.randrange(len(positions))]
            current[src_v]["routes"][src_r].pop(src_s)
            if not current[src_v]["routes"][src_r]:
                current[src_v]["routes"].pop(src_r)
            targets = []
            for v_idx, item in enumerate(current):
                for r_idx, route in enumerate(item.get("routes", [])):
                    for pos in range(len(route) + 1):
                        targets.append((v_idx, r_idx, pos))
                targets.append((v_idx, len(item.get("routes", [])), 0))
            if targets:
                dst_v, dst_r, pos = targets[rng.randrange(len(targets))]
                if dst_r == len(current[dst_v].get("routes", [])):
                    current[dst_v].setdefault("routes", []).append([store_id])
                else:
                    current[dst_v]["routes"][dst_r].insert(pos, store_id)
        elif roll < 0.75 and len(positions) >= 2:
            a = positions[rng.randrange(len(positions))]
            b = positions[rng.randrange(len(positions))]
            if a[:3] != b[:3]:
                av, ar, ai, _ = a
                bv, br, bi, _ = b
                current[av]["routes"][ar][ai], current[bv]["routes"][br][bi] = current[bv]["routes"][br][bi], current[av]["routes"][ar][ai]
        else:
            candidates = [(v, r) for v, item in enumerate(current) for r, route in enumerate(item.get("routes", [])) if len(route) >= 3]
            if candidates:
                v_idx, r_idx = candidates[rng.randrange(len(candidates))]
                route = current[v_idx]["routes"][r_idx]
                i = rng.randrange(0, len(route) - 1)
                j = rng.randrange(i + 1, len(route))
                route[i:j+1] = reversed(route[i:j+1])
        return normalize_routes(current)

    def crossover(parent_a, parent_b, rng, stores, dist, wave, scenario):
        child = clone_state(parent_a)
        inherited = set()
        for item in child:
            if rng.random() < 0.5:
                for route in item.get("routes", []):
                    inherited.update(route)
            else:
                item["routes"] = []
        remaining = []
        for parent in (parent_b, parent_a):
            for item in parent:
                for route in item.get("routes", []):
                    for store_id in route:
                        if store_id not in inherited and store_id not in remaining:
                            remaining.append(store_id)
        return greedy_insert_missing(child, remaining, stores, dist, wave, scenario)

    def tournament(population, rng, size=3):
        best = None
        for _ in range(size):
            candidate = population[rng.randrange(len(population))]
            if best is None or candidate["cost"] < best["cost"]:
                best = candidate
        return best

    population = []
    seen = set()
    for seed_state in [initial_state] + [mutate(initial_state, rng) for _ in range(max(8, population_size * 2))]:
        key = hash_state(seed_state)
        if key in seen:
            continue
        seen.add(key)
        individual = make_individual(seed_state)
        if individual:
            population.append(individual)
        if len(population) >= population_size:
            break
    if not population:
        return {"bestState": vehicles, "traceLog": []}

    best_seen = min(item["cost"] for item in population)
    best_seen_details = evaluate_state_details(min(population, key=lambda item: item["cost"])["state"], stores, dist, wave, scenario) or {}
    stagnant = 0
    trace_log.append({
        "algorithmKey": "ga",
        "scope": "wave",
        "waveId": wave["waveId"],
        "stage": "ga-python-start",
        "bestCost": round(best_seen, 2),
        "costBreakdown": {k: round(float(v or 0.0), 2) for k, v in ((best_seen_details.get("components") or {}).items())},
        "textZh": "",
        "textJa": "",
    })

    for generation in range(generations):
        gen_started_at = time.perf_counter()
        population.sort(key=lambda item: item["cost"])
        next_population = [
            {"state": clone_state(item["state"]), "cost": item["cost"]}
            for item in population[:elite_count]
        ]
        attempts = 0
        max_attempts = population_size * 15
        while len(next_population) < population_size and attempts < max_attempts:
            attempts += 1
            parent_a = tournament(population, rng)
            parent_b = tournament(population, rng)
            child_state = crossover(parent_a["state"], parent_b["state"], rng, stores, dist, wave, scenario)
            if child_state is None:
                child_state = mutate(parent_a["state"], rng)
            if rng.random() < 0.8:
                child_state = mutate(child_state, rng)
            individual = make_individual(child_state)
            if individual:
                next_population.append(individual)
        while len(next_population) < population_size:
            filler = next_population[0] if next_population else population[0]
            next_population.append({"state": clone_state(filler["state"]), "cost": filler["cost"]})
        population = next_population
        population.sort(key=lambda item: item["cost"])
        best_cost = population[0]["cost"]
        best_eval = evaluate_state_details(population[0]["state"], stores, dist, wave, scenario) or {}
        gen_elapsed_ms = (time.perf_counter() - gen_started_at) * 1000.0
        trace_log.append({
            "algorithmKey": "ga",
            "scope": "wave",
            "waveId": wave["waveId"],
            "stage": "ga-python-generation",
            "generation": generation,
            "bestCost": round(best_cost, 2),
            "costBreakdown": {k: round(float(v or 0.0), 2) for k, v in ((best_eval.get("components") or {}).items())},
            "textZh": "",
            "textJa": "",
        })
        if best_seen - best_cost >= improvement_threshold:
            best_seen = best_cost
            stagnant = 0
        else:
            stagnant += 1
            best_seen = min(best_seen, best_cost)
        if stagnant >= stagnation_limit:
            trace_log.append({
                "algorithmKey": "ga",
                "scope": "wave",
                "waveId": wave["waveId"],
                "stage": "ga-python-stop",
                "bestCost": round(best_seen, 2),
                "textZh": "",
                "textJa": "",
            })
            break

    population.sort(key=lambda item: item["cost"])
    final_eval = evaluate_state_details(population[0]["state"], stores, dist, wave, scenario) or {}
    total_elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    trace_log.append({
        "algorithmKey": "ga",
        "scope": "wave",
        "waveId": wave["waveId"],
        "stage": "ga-python-finish",
        "bestCost": round(population[0]["cost"], 2),
        "costBreakdown": {k: round(float(v or 0.0), 2) for k, v in ((final_eval.get("components") or {}).items())},
        "textZh": "",
        "textJa": "",
    })
    return {"bestState": population[0]["state"], "traceLog": trace_log}






def solve(payload):
    return solve_ga(payload)
