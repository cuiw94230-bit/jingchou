"""旧版/兼容层算法实现集合：保留历史阶段算法和兼容逻辑。"""`n`nimport json
import math
import random
import re
import time
from copy import deepcopy
from functools import lru_cache

try:
    import cupy as cp
    GPU_AVAILABLE = True
except Exception:
    GPU_AVAILABLE = False

try:
    from skopt import gp_minimize
    from skopt.space import Real, Integer
    SKOPT_AVAILABLE = True
except Exception:
    SKOPT_AVAILABLE = False

DC_ID = "DC"
IGNORE_CAPACITY_CONSTRAINT = False
from .common_ops import (
    clone_state,
    hash_state,
    normalize_routes,
    route_distance,
    best_insert_for_vehicle,
    greedy_insert_missing,
    vrptw_insert_missing,
    savings_insert_missing,
    empty_like_state,
    collect_unique_stores,
    score_for_savings_order,
)
from .constraints import check_plan_constraints
from .objective import score_plan_components
from .evaluator import evaluate_plan, evaluate_state, evaluate_state_details
from .constructive import solve_constructive, build_initial_from_order, generate_diverse_initial_state
from .vehicle_driven import solve_vehicle_driven

class AntColonyOptimizer:
    """doc"""
    
    def __init__(self, stores, dist, wave, scenario, vehicles, rng, initial_state=None):
        self.stores = stores
        self.dist = dist
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
        self.rng = rng
        
        self.alpha = 1.0
        self.beta = 2.0
        self.rho = 0.25
        self.q0 = 0.85
        self.ant_count = 6
        self.elite_count = 1
        self.stagnation_limit = 4
        
        self.pheromone = {}
        self._init_pheromone_with_initial(initial_state)
    
    def _get_pheromone(self, i, j):
        return self.pheromone.get((i, j), 0.5)
    
    def _set_pheromone(self, i, j, value):
        self.pheromone[(i, j)] = max(0.01, min(50.0, value))
    
    def _init_pheromone_with_initial(self, initial_state):
        all_stores = list(self.stores.keys())
        
        for i in ["DC"] + all_stores:
            for j in ["DC"] + all_stores:
                if i != j:
                    self._set_pheromone(i, j, 0.5)
        
        if initial_state:
            for item in initial_state:
                for route in item.get("routes", []):
                    seq = ["DC"] + route + ["DC"]
                    for k in range(len(seq) - 1):
                        current = self._get_pheromone(seq[k], seq[k+1])
                        self._set_pheromone(seq[k], seq[k+1], current + 1.0)
    
    def _eta(self, i, j):
        dist_val = self.dist.get(i, {}).get(j, 1.0)
        return 1.0 / (dist_val + 0.1)
    
    def _build_route_fast(self, unassigned, vehicle):
        route = []
        current = "DC"
        time = self.wave["startMin"]
        load = 0.0
        
        unassigned_list = list(unassigned)
        
        while unassigned_list:
            candidates = []
            for store_id in unassigned_list[:25]:
                store = self.stores[store_id]
                travel_time = self.dist[current][store_id] / max(vehicle["speed"], 1) * 60
                arrival = time + travel_time
                
                if arrival > store["latestAllowedArrivalMin"] + 90:
                    continue
                if (not IGNORE_CAPACITY_CONSTRAINT) and load + store["boxes"] > vehicle["capacity"]:
                    continue
                # 冷链约束已停用：不再按 coldRatio/canCarryCold 过滤候选门店
                
                tau = self._get_pheromone(current, store_id)
                eta = self._eta(current, store_id)
                candidates.append((store_id, tau * eta))
            
            if not candidates:
                chosen = unassigned_list[0]
            else:
                if self.rng.random() < self.q0:
                    chosen = max(candidates, key=lambda x: x[1])[0]
                else:
                    total = sum(c[1] for c in candidates)
                    if total <= 0:
                        chosen = candidates[0][0]
                    else:
                        r = self.rng.random() * total
                        acc = 0
                        for store_id, weight in candidates:
                            acc += weight
                            if acc >= r:
                                chosen = store_id
                                break
                        else:
                            chosen = candidates[-1][0]
            
            store = self.stores[chosen]
            travel_time = self.dist[current][chosen] / max(vehicle["speed"], 1) * 60
            route.append(chosen)
            unassigned_list.remove(chosen)
            time = time + travel_time + store["actualServiceMinutes"]
            load += store["boxes"]
            current = chosen
        
        return route
    
    def _global_update_pheromone(self, solutions):
        for key in list(self.pheromone.keys()):
            self.pheromone[key] *= (1 - self.rho)
        
        if not solutions:
            return
        
        best = min(solutions, key=lambda x: x["cost"])
        delta = 1.0 / max(best["cost"], 0.1)
        for i, j in best["edges"]:
            current = self._get_pheromone(i, j)
            self._set_pheromone(i, j, current + delta)
    
    def optimize(self, initial_state, max_iterations=20):
        best_state = initial_state
        best_cost = evaluate_state(best_state, self.stores, self.dist, self.wave, self.scenario)
        if best_cost is None:
            best_cost = float('inf')
        
        trace_log = []
        stagnation = 0
        
        for gen in range(max_iterations):
            gen_started = time.perf_counter()
            ant_solutions = []
            
            for ant in range(self.ant_count):
                state = clone_state(self.vehicles)
                unassigned = set(self.stores.keys())
                
                for vehicle_idx, vehicle in enumerate(state):
                    route = self._build_route_fast(unassigned, vehicle)
                    state[vehicle_idx]["routes"] = [route] if route else []
                    for store_id in route:
                        unassigned.discard(store_id)
                    if not unassigned:
                        break
                
                cost = evaluate_state(state, self.stores, self.dist, self.wave, self.scenario)
                if cost is not None:
                    edges = []
                    for item in state:
                        for route in item.get("routes", []):
                            seq = ["DC"] + route + ["DC"]
                            for k in range(len(seq) - 1):
                                edges.append((seq[k], seq[k+1]))
                    ant_solutions.append({
                        "state": state,
                        "cost": cost,
                        "edges": edges
                    })
                    
                    if cost < best_cost:
                        best_cost = cost
                        best_state = clone_state(state)
                        stagnation = 0
                        trace_log.append({
                            "iteration": gen,
                            "ant": ant,
                            "action": "aco_improved",
                            "oldCost": round(best_cost, 2),
                            "newCost": round(cost, 2),
                            "improved": True,
                        })
            
            if ant_solutions:
                self._global_update_pheromone(ant_solutions)
                gen_best = min(s["cost"] for s in ant_solutions)
                if gen_best >= best_cost:
                    stagnation += 1
                else:
                    stagnation = 0
            
            gen_elapsed = (time.perf_counter() - gen_started) * 1000
            trace_log.append({
                "iteration": gen,
                "action": "generation",
                "cost": round(best_cost, 2),
                "elapsed_ms": round(gen_elapsed, 1),
            })
            
            if stagnation >= self.stagnation_limit:
                trace_log.append({
                    "iteration": gen,
                    "action": "early_stop",
                    "reason": f"杩炵画 {self.stagnation_limit} 浠ｆ棤鏀硅繘",
                })
                break
        
        return best_state, trace_log


# ============================================================
# 淇鐗堢矑瀛愮兢绠楁硶 (PSO)
# ============================================================
class ParticleSwarmOptimizer:
    """doc"""
    
    def __init__(self, stores, dist, wave, scenario, vehicles, rng):
        self.stores = stores
        self.dist = dist
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
        self.rng = rng
        
        self.w = 0.7
        self.c1 = 1.5
        self.c2 = 1.5
        self.particle_count = 15
        self.mutation_rate = 0.15
        self.stagnation_limit = 10
    
    def _state_to_order(self, state):
        order = []
        for item in state:
            for route in item.get("routes", []):
                order.extend(route)
        assigned = set(order)
        for store in self.stores.keys():
            if store not in assigned:
                order.append(store)
        return order
    
    def _order_to_state_safe(self, order):
        state = clone_state(self.vehicles)
        for item in state:
            item["routes"] = []
        
        vehicle_idx = 0
        for store_id in order:
            if store_id not in self.stores:
                continue
            
            placed = False
            for attempt in range(len(state)):
                v = state[vehicle_idx]
                current_load = 0.0
                for route in v.get("routes", []):
                    for s in route:
                        if s in self.stores:
                            current_load += self.stores[s]["boxes"]
                
                store_boxes = self.stores[store_id]["boxes"]
                
                if IGNORE_CAPACITY_CONSTRAINT or current_load + store_boxes <= v["capacity"]:
                    # 冷链约束已停用：不再按 coldRatio/canCarryCold 过滤车辆
                    
                    if not v.get("routes"):
                        v["routes"] = [[]]
                    v["routes"][0].append(store_id)
                    placed = True
                    break
                vehicle_idx = (vehicle_idx + 1) % len(state)
            
            if not placed:
                if not state[vehicle_idx].get("routes"):
                    state[vehicle_idx]["routes"] = []
                state[vehicle_idx]["routes"].append([store_id])
            
            vehicle_idx = (vehicle_idx + 1) % len(state)
        
        return normalize_routes(state)
    
    def _swap_operator(self, order):
        new_order = list(order)
        if len(new_order) < 2:
            return new_order
        i, j = self.rng.sample(range(len(new_order)), 2)
        new_order[i], new_order[j] = new_order[j], new_order[i]
        return new_order
    
    def _mutate(self, order):
        new_order = list(order)
        if len(new_order) < 3:
            return new_order
        i = self.rng.randrange(0, len(new_order) - 2)
        j = self.rng.randrange(i + 1, len(new_order))
        new_order[i:j+1] = reversed(new_order[i:j+1])
        return new_order
    
    def _update_velocity(self, vel, pbest, gbest, current):
        new_vel = list(vel)
        for i in range(min(len(pbest), len(current))):
            if pbest[i] != current[i]:
                try:
                    idx = current.index(pbest[i])
                    if idx != i:
                        new_vel.append(('swap', i, idx))
                except ValueError:
                    pass
        for i in range(min(len(gbest), len(current))):
            if gbest[i] != current[i]:
                try:
                    idx = current.index(gbest[i])
                    if idx != i:
                        new_vel.append(('swap', i, idx))
                except ValueError:
                    pass
        if len(new_vel) > 40:
            new_vel = new_vel[-40:]
        return new_vel
    
    def _apply_velocity(self, order, velocity):
        result = list(order)
        for op in velocity:
            if op[0] == 'swap' and len(result) > op[1] and len(result) > op[2]:
                result[op[1]], result[op[2]] = result[op[2]], result[op[1]]
        return result
    
    def _evaluate_safe(self, state):
        cost = evaluate_state(state, self.stores, self.dist, self.wave, self.scenario)
        if cost is None or math.isinf(cost) or math.isnan(cost):
            return 1e9
        return cost
    
    def optimize(self, initial_state, max_iterations=40):
        initial_order = self._state_to_order(initial_state)
        
        particles = []
        velocities = []
        trace_log = []
        
        for _ in range(self.particle_count):
            order = list(initial_order)
            self.rng.shuffle(order)
            state = self._order_to_state_safe(order)
            cost = self._evaluate_safe(state)
            
            particles.append({
                "position": order,
                "cost": cost,
                "pbest_pos": list(order),
                "pbest_cost": cost
            })
            velocities.append([])
        
        gbest = min(particles, key=lambda p: p["cost"])
        gbest_pos = list(gbest["position"])
        gbest_cost = gbest["cost"]
        best_state = self._order_to_state_safe(gbest_pos)
        
        stagnation = 0
        
        for gen in range(max_iterations):
            gen_started = time.perf_counter()
            gen_best = float('inf')
            
            for i, particle in enumerate(particles):
                new_vel = self._update_velocity(
                    velocities[i],
                    particle["pbest_pos"],
                    gbest_pos,
                    particle["position"]
                )
                velocities[i] = new_vel
                
                new_pos = self._apply_velocity(particle["position"], new_vel)
                
                if self.rng.random() < self.mutation_rate:
                    new_pos = self._mutate(new_pos)
                
                particles[i]["position"] = new_pos
                
                new_state = self._order_to_state_safe(new_pos)
                new_cost = self._evaluate_safe(new_state)
                particles[i]["cost"] = new_cost
                
                if new_cost < particle["pbest_cost"]:
                    particles[i]["pbest_pos"] = list(new_pos)
                    particles[i]["pbest_cost"] = new_cost
                
                if new_cost < gbest_cost:
                    gbest_cost = new_cost
                    gbest_pos = list(new_pos)
                    best_state = clone_state(new_state)
                    stagnation = 0
                    trace_log.append({
                        "iteration": gen,
                        "particle": i,
                        "action": "pso_improved",
                        "cost": round(gbest_cost, 2),
                        "improved": True,
                    })
                
                if new_cost < gen_best:
                    gen_best = new_cost
            
            if gen_best >= gbest_cost:
                stagnation += 1
            else:
                stagnation = 0
            
            gen_elapsed = (time.perf_counter() - gen_started) * 1000
            cost_display = round(gbest_cost, 2) if gbest_cost < 1e8 else 99999.99
            trace_log.append({
                "iteration": gen,
                "action": "generation",
                "cost": cost_display,
                "elapsed_ms": round(gen_elapsed, 1),
            })
            
            if stagnation >= self.stagnation_limit:
                trace_log.append({
                    "iteration": gen,
                    "action": "early_stop",
                    "reason": f"杩炵画 {self.stagnation_limit} 浠ｆ棤鏀硅繘",
                })
                break
        
        return best_state, trace_log


# ============================================================
# 浼樺寲鐗堟ā鎷熼€€鐏?(SA)
# ============================================================
class SimulatedAnnealing:
    """doc"""
    
    def __init__(self, stores, dist, wave, scenario, vehicles, rng):
        self.stores = stores
        self.dist = dist
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
        self.rng = rng
        
        self.initial_temp = 5000.0
        self.final_temp = 0.1
        self.cooling_rate = 0.97
        self.iterations_per_temp = 80
    
    def _relocate(self, state):
        new_state = clone_state(state)
        positions = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                for s_idx, store in enumerate(route):
                    positions.append((v_idx, r_idx, s_idx, store))
        if len(positions) < 1:
            return new_state
        src = self.rng.choice(positions)
        new_state[src[0]]["routes"][src[1]].pop(src[2])
        if not new_state[src[0]]["routes"][src[1]]:
            new_state[src[0]]["routes"].pop(src[1])
        
        targets = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                for pos in range(len(route) + 1):
                    targets.append((v_idx, r_idx, pos))
            targets.append((v_idx, len(item.get("routes", [])), 0))
        if targets:
            dst = self.rng.choice(targets)
            if dst[1] == len(new_state[dst[0]].get("routes", [])):
                new_state[dst[0]].setdefault("routes", []).append([src[3]])
            else:
                new_state[dst[0]]["routes"][dst[1]].insert(dst[2], src[3])
        return normalize_routes(new_state)
    
    def _swap(self, state):
        new_state = clone_state(state)
        positions = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                for s_idx, store in enumerate(route):
                    positions.append((v_idx, r_idx, s_idx, store))
        if len(positions) < 2:
            return new_state
        a, b = self.rng.sample(positions, 2)
        if a[:3] == b[:3]:
            return new_state
        new_state[a[0]]["routes"][a[1]][a[2]], new_state[b[0]]["routes"][b[1]][b[2]] = \
            new_state[b[0]]["routes"][b[1]][b[2]], new_state[a[0]]["routes"][a[1]][a[2]]
        return normalize_routes(new_state)
    
    def _two_opt(self, state):
        new_state = clone_state(state)
        candidates = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                if len(route) >= 3:
                    candidates.append((v_idx, r_idx))
        if not candidates:
            return new_state
        v_idx, r_idx = self.rng.choice(candidates)
        route = new_state[v_idx]["routes"][r_idx]
        i = self.rng.randrange(0, len(route) - 1)
        j = self.rng.randrange(i + 1, len(route))
        route[i:j+1] = reversed(route[i:j+1])
        return normalize_routes(new_state)
    
    def _neighbor(self, state):
        ops = [self._relocate, self._swap, self._two_opt]
        weights = [0.4, 0.35, 0.25]
        return self.rng.choices(ops, weights=weights)[0](state)
    
    def optimize(self, initial_state, max_iterations=600):
        current_state = clone_state(initial_state)
        current_cost = evaluate_state(current_state, self.stores, self.dist, self.wave, self.scenario)
        if current_cost is None:
            return initial_state, []
        
        best_state = clone_state(current_state)
        best_cost = current_cost
        
        temp = self.initial_temp
        iteration = 0
        stagnation = 0
        
        trace_log = []
        
        while temp > self.final_temp and iteration < max_iterations:
            for _ in range(self.iterations_per_temp):
                iter_started = time.perf_counter()
                candidate = self._neighbor(current_state)
                candidate_cost = evaluate_state(candidate, self.stores, self.dist, self.wave, self.scenario)
                if candidate_cost is None:
                    continue
                
                delta = candidate_cost - current_cost
                accepted = delta < 0 or self.rng.random() < math.exp(-delta / temp)
                
                if accepted:
                    current_state = candidate
                    current_cost = candidate_cost
                    
                    if current_cost < best_cost:
                        best_state = clone_state(current_state)
                        best_cost = current_cost
                        stagnation = 0
                        trace_log.append({
                            "iteration": iteration,
                            "action": "sa_improved",
                            "oldCost": round(current_cost, 2),
                            "newCost": round(best_cost, 2),
                            "temp": round(temp, 2),
                            "improved": True,
                        })
                    else:
                        stagnation += 1
                
                iter_elapsed = (time.perf_counter() - iter_started) * 1000
                if iteration % 30 == 0:
                    trace_log.append({
                        "iteration": iteration,
                        "action": "sa_iteration",
                        "current_cost": round(current_cost, 2),
                        "best_cost": round(best_cost, 2),
                        "temp": round(temp, 2),
                        "accepted": accepted,
                        "elapsed_ms": round(iter_elapsed, 1),
                    })
                
                iteration += 1
                if iteration >= max_iterations:
                    break
            
            temp *= self.cooling_rate
            
            if stagnation > 200:
                temp = self.initial_temp
                stagnation = 0
                trace_log.append({
                    "iteration": iteration,
                    "action": "sa_restart",
                })
        
        return best_state, trace_log


# ============================================================
# 浼樺寲鐗堝ぇ閭诲煙鎼滅储 (LNS)
# ============================================================
class LargeNeighborhoodSearch:
    """doc"""
    
    def __init__(self, stores, dist, wave, scenario, vehicles, rng):
        self.stores = stores
        self.dist = dist
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
        self.rng = rng
        
        self.destroy_ratio = 0.35
        self.temperature = 100.0
        self.cooling_rate = 0.97
        self.stagnation_limit = 15
    
    def _destroy_random(self, state, ratio):
        new_state = clone_state(state)
        all_stores = []
        for item in new_state:
            for route in item.get("routes", []):
                all_stores.extend(route)
        remove_count = max(2, int(len(all_stores) * ratio))
        to_remove = set(self.rng.sample(all_stores, min(remove_count, len(all_stores))))
        
        for item in new_state:
            new_routes = []
            for route in item.get("routes", []):
                filtered = [s for s in route if s not in to_remove]
                if filtered:
                    new_routes.append(filtered)
            item["routes"] = new_routes
        
        return new_state, list(to_remove)
    
    def _destroy_worst(self, state, ratio):
        new_state = clone_state(state)
        store_costs = {}
        for item in new_state:
            for route in item.get("routes", []):
                for i, store_id in enumerate(route):
                    prev = "DC" if i == 0 else route[i-1]
                    nxt = "DC" if i == len(route)-1 else route[i+1]
                    cost = self.dist[prev][store_id] + self.dist[store_id][nxt]
                    store_costs[store_id] = cost
        
        sorted_stores = sorted(store_costs.items(), key=lambda x: -x[1])
        remove_count = max(2, int(len(store_costs) * ratio))
        to_remove = set([s[0] for s in sorted_stores[:remove_count]])
        
        for item in new_state:
            new_routes = []
            for route in item.get("routes", []):
                filtered = [s for s in route if s not in to_remove]
                if filtered:
                    new_routes.append(filtered)
            item["routes"] = new_routes
        
        return new_state, list(to_remove)
    
    def _repair_greedy(self, state, removed_stores):
        current = clone_state(state)
        for store_id in removed_stores:
            best_pos = None
            best_delta = float('inf')
            for v_idx, item in enumerate(current):
                for r_idx, route in enumerate(item.get("routes", [])):
                    for pos in range(len(route) + 1):
                        prev = "DC" if pos == 0 else route[pos - 1]
                        nxt = "DC" if pos == len(route) else route[pos]
                        delta = self.dist[prev][store_id] + self.dist[store_id][nxt] - self.dist[prev][nxt]
                        if delta < best_delta:
                            best_delta = delta
                            best_pos = (v_idx, r_idx, pos)
            if best_pos:
                v_idx, r_idx, pos = best_pos
                if r_idx >= len(current[v_idx].get("routes", [])):
                    current[v_idx].setdefault("routes", []).append([store_id])
                else:
                    current[v_idx]["routes"][r_idx].insert(pos, store_id)
        return normalize_routes(current)
    
    def _repair_regret(self, state, removed_stores, k=2):
        current = clone_state(state)
        remaining = list(removed_stores)
        
        while remaining:
            best_store = None
            best_regret = -float('inf')
            best_insert = None
            
            for store_id in remaining:
                inserts = []
                for v_idx, item in enumerate(current):
                    for r_idx, route in enumerate(item.get("routes", [])):
                        for pos in range(len(route) + 1):
                            prev = "DC" if pos == 0 else route[pos - 1]
                            nxt = "DC" if pos == len(route) else route[pos]
                            delta = self.dist[prev][store_id] + self.dist[store_id][nxt] - self.dist[prev][nxt]
                            inserts.append((delta, v_idx, r_idx, pos))
                inserts.sort(key=lambda x: x[0])
                if len(inserts) >= k:
                    regret = inserts[k-1][0] - inserts[0][0]
                else:
                    regret = inserts[-1][0] - inserts[0][0] if inserts else 0
                if regret > best_regret and inserts:
                    best_regret = regret
                    best_store = store_id
                    best_insert = inserts[0]
            
            if best_store and best_insert:
                delta, v_idx, r_idx, pos = best_insert
                if r_idx >= len(current[v_idx].get("routes", [])):
                    current[v_idx].setdefault("routes", []).append([best_store])
                else:
                    current[v_idx]["routes"][r_idx].insert(pos, best_store)
                remaining.remove(best_store)
        
        return normalize_routes(current)
    
    def optimize(self, initial_state, max_iterations=60):
        current_state = clone_state(initial_state)
        current_cost = evaluate_state(current_state, self.stores, self.dist, self.wave, self.scenario)
        if current_cost is None:
            return initial_state, []
        
        best_state = clone_state(current_state)
        best_cost = current_cost
        
        trace_log = []
        temp = self.temperature
        stagnation = 0
        
        for iteration in range(max_iterations):
            iter_started = time.perf_counter()
            
            if self.rng.random() < 0.5:
                destroyed, removed = self._destroy_random(current_state, self.destroy_ratio)
                destroy_type = "random"
            else:
                destroyed, removed = self._destroy_worst(current_state, self.destroy_ratio)
                destroy_type = "worst"
            
            if self.rng.random() < 0.6:
                candidate = self._repair_greedy(destroyed, removed)
                repair_type = "greedy"
            else:
                candidate = self._repair_regret(destroyed, removed, k=2)
                repair_type = "regret"
            
            candidate_cost = evaluate_state(candidate, self.stores, self.dist, self.wave, self.scenario)
            
            improved = False
            if candidate_cost and candidate_cost < current_cost:
                current_state = candidate
                current_cost = candidate_cost
                improved = True
                stagnation = 0
                
                if current_cost < best_cost:
                    best_state = clone_state(current_state)
                    best_cost = current_cost
                    trace_log.append({
                        "iteration": iteration,
                        "action": "lns_improved",
                        "destroy": destroy_type,
                        "repair": repair_type,
                        "cost": round(best_cost, 2),
                        "improved": True,
                    })
            elif candidate_cost and self.rng.random() < math.exp((current_cost - candidate_cost) / temp):
                current_state = candidate
                current_cost = candidate_cost
                improved = True
            
            if not improved:
                stagnation += 1
            
            temp *= self.cooling_rate
            
            iter_elapsed = (time.perf_counter() - iter_started) * 1000
            if iteration % 10 == 0:
                trace_log.append({
                    "iteration": iteration,
                    "action": "lns_iteration",
                    "destroy": destroy_type,
                    "repair": repair_type,
                    "improved": improved,
                    "current_cost": round(current_cost, 2),
                    "best_cost": round(best_cost, 2),
                    "temp": round(temp, 2),
                    "elapsed_ms": round(iter_elapsed, 1),
                })
            
            if stagnation >= self.stagnation_limit:
                trace_log.append({
                    "iteration": iteration,
                    "action": "early_stop",
                    "reason": f"杩炵画 {self.stagnation_limit} 杞棤鏀硅繘",
                })
                break
        
        return best_state, trace_log


# ============================================================
# 浼樺寲鐨勬贩鍚堢畻娉?(Hybrid)
# ============================================================
class HybridOptimizer:
    """doc"""
    
    def __init__(self, stores, dist, wave, scenario, vehicles, rng):
        self.stores = stores
        self.dist = dist
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
        self.rng = rng
    
    def optimize(self, initial_state, max_iterations=60):
        current_state = clone_state(initial_state)
        
        trace_log = []
        
        sa = SimulatedAnnealing(self.stores, self.dist, self.wave, self.scenario, self.vehicles, self.rng)
        current_state, sa_log = sa.optimize(current_state, max_iterations=300)
        trace_log.extend(sa_log)
        
        current_cost = evaluate_state(current_state, self.stores, self.dist, self.wave, self.scenario)
        best_state = clone_state(current_state)
        best_cost = current_cost
        
        lns = LargeNeighborhoodSearch(self.stores, self.dist, self.wave, self.scenario, self.vehicles, self.rng)
        current_state, lns_log = lns.optimize(current_state, max_iterations=40)
        trace_log.extend(lns_log)
        
        current_cost = evaluate_state(current_state, self.stores, self.dist, self.wave, self.scenario)
        if current_cost and current_cost < best_cost:
            best_state = clone_state(current_state)
            best_cost = current_cost
        
        tabu = TabuSearch(self.stores, self.dist, self.wave, self.scenario, self.vehicles, self.rng)
        current_state, tabu_log = tabu.optimize(best_state, max_iterations=30)
        trace_log.extend(tabu_log)
        
        current_cost = evaluate_state(current_state, self.stores, self.dist, self.wave, self.scenario)
        if current_cost and current_cost < best_cost:
            best_state = clone_state(current_state)
            best_cost = current_cost
        
        return best_state, trace_log


# ============================================================
# 浼樺寲鐨勭蹇屾悳绱?(TS)
# ============================================================
class TabuSearch:
    """doc"""
    
    def __init__(self, stores, dist, wave, scenario, vehicles, rng):
        self.stores = stores
        self.dist = dist
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
        self.rng = rng
        self.tabu_tenure = 12
    
    def _relocate(self, state):
        new_state = clone_state(state)
        positions = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                for s_idx, store in enumerate(route):
                    positions.append((v_idx, r_idx, s_idx, store))
        if len(positions) < 1:
            return new_state
        src = self.rng.choice(positions)
        new_state[src[0]]["routes"][src[1]].pop(src[2])
        if not new_state[src[0]]["routes"][src[1]]:
            new_state[src[0]]["routes"].pop(src[1])
        
        targets = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                for pos in range(len(route) + 1):
                    targets.append((v_idx, r_idx, pos))
            targets.append((v_idx, len(item.get("routes", [])), 0))
        if targets:
            dst = self.rng.choice(targets)
            if dst[1] == len(new_state[dst[0]].get("routes", [])):
                new_state[dst[0]].setdefault("routes", []).append([src[3]])
            else:
                new_state[dst[0]]["routes"][dst[1]].insert(dst[2], src[3])
        return normalize_routes(new_state)
    
    def _swap(self, state):
        new_state = clone_state(state)
        positions = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                for s_idx, store in enumerate(route):
                    positions.append((v_idx, r_idx, s_idx, store))
        if len(positions) < 2:
            return new_state
        a, b = self.rng.sample(positions, 2)
        if a[:3] == b[:3]:
            return new_state
        new_state[a[0]]["routes"][a[1]][a[2]], new_state[b[0]]["routes"][b[1]][b[2]] = \
            new_state[b[0]]["routes"][b[1]][b[2]], new_state[a[0]]["routes"][a[1]][a[2]]
        return normalize_routes(new_state)
    
    def _two_opt(self, state):
        new_state = clone_state(state)
        candidates = []
        for v_idx, item in enumerate(new_state):
            for r_idx, route in enumerate(item.get("routes", [])):
                if len(route) >= 3:
                    candidates.append((v_idx, r_idx))
        if not candidates:
            return new_state
        v_idx, r_idx = self.rng.choice(candidates)
        route = new_state[v_idx]["routes"][r_idx]
        i = self.rng.randrange(0, len(route) - 1)
        j = self.rng.randrange(i + 1, len(route))
        route[i:j+1] = reversed(route[i:j+1])
        return normalize_routes(new_state)
    
    def _neighbor(self, state):
        ops = [self._relocate, self._swap, self._two_opt]
        weights = [0.4, 0.35, 0.25]
        return self.rng.choices(ops, weights=weights)[0](state)
    
    def optimize(self, initial_state, max_iterations=80):
        current_state = clone_state(initial_state)
        current_cost = evaluate_state(current_state, self.stores, self.dist, self.wave, self.scenario)
        if current_cost is None:
            return initial_state, []
        
        best_state = clone_state(current_state)
        best_cost = current_cost
        tabu_list = []
        
        trace_log = []
        
        for iteration in range(max_iterations):
            iter_started = time.perf_counter()
            best_candidate = None
            best_candidate_cost = float('inf')
            best_move = None
            
            for _ in range(40):
                candidate = self._neighbor(current_state)
                move_key = hash(str(candidate))
                
                if move_key in tabu_list:
                    candidate_cost = evaluate_state(candidate, self.stores, self.dist, self.wave, self.scenario)
                    if candidate_cost and candidate_cost >= best_cost:
                        continue
                
                candidate_cost = evaluate_state(candidate, self.stores, self.dist, self.wave, self.scenario)
                if candidate_cost and candidate_cost < best_candidate_cost:
                    best_candidate_cost = candidate_cost
                    best_candidate = candidate
                    best_move = move_key
            
            if best_candidate:
                current_state = best_candidate
                current_cost = best_candidate_cost
                tabu_list.append(best_move)
                if len(tabu_list) > self.tabu_tenure:
                    tabu_list.pop(0)
                
                if current_cost < best_cost:
                    best_state = clone_state(current_state)
                    best_cost = current_cost
                    trace_log.append({
                        "iteration": iteration,
                        "action": "tabu_improved",
                        "oldCost": round(current_cost, 2),
                        "newCost": round(best_cost, 2),
                        "improved": True,
                    })
            
            iter_elapsed = (time.perf_counter() - iter_started) * 1000
            if iteration % 20 == 0:
                trace_log.append({
                    "iteration": iteration,
                    "action": "tabu_iteration",
                    "current_cost": round(current_cost, 2),
                    "best_cost": round(best_cost, 2),
                    "tabu_size": len(tabu_list),
                    "elapsed_ms": round(iter_elapsed, 1),
                })
        
        return best_state, trace_log


# ============================================================
# 鑷姩璋冨弬鍣?
# ============================================================
class AutoTuner:
    """doc"""
    
    def __init__(self, stores, dist, wave, scenario, vehicles):
        self.stores = stores
        self.dist = dist
        self.wave = wave
        self.scenario = scenario
        self.vehicles = vehicles
    
    def tune_aco(self, payload, n_trials=10):
        if not SKOPT_AVAILABLE:
            return self._grid_search_aco(payload)
        
        def objective(params):
            alpha, beta, rho, ant_count = params
            rng = random.Random(42)
            
            constructive_payload = {
                "scenario": self.scenario,
                "wave": self.wave,
                "stores": payload["stores"],
                "vehicles": self.vehicles,
                "dist": self.dist,
            }
            constructive_result = solve_constructive(constructive_payload, "savings")
            initial_state = constructive_result["bestState"]
            
            optimizer = AntColonyOptimizer(
                self.stores, self.dist, self.wave, self.scenario, 
                self.vehicles, rng, initial_state
            )
            optimizer.alpha = alpha
            optimizer.beta = beta
            optimizer.rho = rho
            optimizer.ant_count = int(ant_count)
            optimizer.q0 = 0.85
            
            best_state, _ = optimizer.optimize(initial_state, max_iterations=10)
            cost = evaluate_state(best_state, self.stores, self.dist, self.wave, self.scenario)
            return cost if cost is not None else 1e9
        
        space = [
            Real(0.5, 2.0, name='alpha'),
            Real(1.0, 4.0, name='beta'),
            Real(0.05, 0.3, name='rho'),
            Integer(4, 12, name='ant_count')
        ]
        
        result = gp_minimize(objective, space, n_calls=n_trials, random_state=42, verbose=False)
        return {
            "alpha": result.x[0],
            "beta": result.x[1],
            "rho": result.x[2],
            "ant_count": int(result.x[3]),
        }
    
    def _grid_search_aco(self, payload):
        return {"alpha": 1.2, "beta": 1.8, "rho": 0.2, "ant_count": 6}


# ============================================================
# 涓诲叆鍙ｅ嚱鏁?
# ============================================================
def solve_metaheuristic_v2(payload):
    started_at = time.perf_counter()
    rng = random.Random(payload.get("randomSeed", 203))
    algorithm_key = str(payload.get("algorithmKey") or "tabu").strip().lower()
    
    if algorithm_key in ("vrptw", "savings"):
        return solve_constructive(payload, algorithm_key)
    if algorithm_key in ("vehicle", "vehicle_driven"):
        return solve_vehicle_driven(payload)
    
    scenario = payload["scenario"]
    wave = payload["wave"]
    stores = {item["id"]: item for item in payload["stores"]}
    vehicles = payload["vehicles"]
    dist = payload["dist"]
    
    # 姣忎釜绠楁硶浣跨敤鍚勮嚜鐙珛鐨勫垵濮嬭В
    initial_state = generate_diverse_initial_state(payload, algorithm_key, rng)
    initial_cost = evaluate_state(initial_state, stores, dist, wave, scenario) or 0.0
    
    trace_log = []
    
    labels = {
        "hybrid": ("混合VRPTW", "ハイブリッドVRPTW"),
        "tabu": ("禁忌搜索", "タブー探索"),
        "lns": ("大邻域搜索", "大近傍探索"),
        "sa": ("模拟退火", "焼きなまし"),
        "aco": ("蚁群算法", "蟻コロニー最適化"),
        "pso": ("粒子群算法", "粒子群最適化"),
    }
    label_zh, label_ja = labels.get(algorithm_key, ("后台优化", "バックエンド最適化"))
    
    init_strategy = {
        "sa": "闅忔満椤哄簭",
        "tabu": "闅忔満椤哄簭", 
        "lns": "璺濈浼樺厛",
        "aco": "时间窗优先",
        "pso": "瀹屽叏闅忔満",
        "hybrid": "Clark-Wright节约法",
    }.get(algorithm_key, "Clark-Wright节约法")
    
    trace_log.append({
        "algorithmKey": algorithm_key,
        "scope": "wave",
        "waveId": wave["waveId"],
        "stage": f"{algorithm_key}-python-start",
        "initialCost": round(initial_cost, 2),
        "initStrategy": init_strategy,
        "textZh": "",
        "textJa": "",
    })
    
    # 閫夋嫨浼樺寲鍣?
    if algorithm_key == "aco":
        store_count = len(stores)
        
        if store_count <= 80:
            tuner = AutoTuner(stores, dist, wave, scenario, vehicles)
            best_params = tuner.tune_aco(payload, n_trials=10)
            alpha = best_params.get("alpha", 1.2)
            beta = best_params.get("beta", 1.8)
            rho = best_params.get("rho", 0.2)
            ant_cnt = min(best_params.get("ant_count", 6), 10)
            max_iter = 20
            trace_log.append({
                "algorithmKey": algorithm_key,
                "textZh": "",
            })
        else:
            alpha, beta, rho, ant_cnt = 1.2, 1.8, 0.25, 6
            max_iter = 18
            trace_log.append({
                "algorithmKey": algorithm_key,
                "textZh": "",
            })
        
        optimizer = AntColonyOptimizer(stores, dist, wave, scenario, vehicles, rng, initial_state)
        optimizer.alpha = alpha
        optimizer.beta = beta
        optimizer.rho = rho
        optimizer.ant_count = ant_cnt
        optimizer.q0 = 0.85
        optimizer.stagnation_limit = 4
        
        best_state, optimizer_log = optimizer.optimize(initial_state, max_iter)
        
    elif algorithm_key == "pso":
        optimizer = ParticleSwarmOptimizer(stores, dist, wave, scenario, vehicles, rng)
        max_iterations = 40
        try:
            best_state, optimizer_log = optimizer.optimize(initial_state, max_iterations)
        except Exception as pso_error:
            trace_log.append({
                "algorithmKey": algorithm_key,
                "scope": "wave",
                "waveId": wave["waveId"],
                "stage": f"{algorithm_key}-python-error",
                "error": str(pso_error),
                "textZh": "",
            })
            best_state = initial_state
            optimizer_log = []
        
    elif algorithm_key == "sa":
        optimizer = SimulatedAnnealing(stores, dist, wave, scenario, vehicles, rng)
        max_iterations = 600
        best_state, optimizer_log = optimizer.optimize(initial_state, max_iterations)
        
    elif algorithm_key == "hybrid":
        optimizer = HybridOptimizer(stores, dist, wave, scenario, vehicles, rng)
        max_iterations = 60
        best_state, optimizer_log = optimizer.optimize(initial_state, max_iterations)
        
    elif algorithm_key == "tabu":
        optimizer = TabuSearch(stores, dist, wave, scenario, vehicles, rng)
        max_iterations = 80
        best_state, optimizer_log = optimizer.optimize(initial_state, max_iterations)
        
    elif algorithm_key == "lns":
        optimizer = LargeNeighborhoodSearch(stores, dist, wave, scenario, vehicles, rng)
        max_iterations = 60
        best_state, optimizer_log = optimizer.optimize(initial_state, max_iterations)
        
    else:
        return solve_constructive(payload, algorithm_key)
    
    best_cost = evaluate_state(best_state, stores, dist, wave, scenario) or initial_cost
    
    for log_entry in optimizer_log:
        log_entry["algorithmKey"] = algorithm_key
        log_entry["scope"] = "wave"
        log_entry["waveId"] = wave["waveId"]
        log_entry["stage"] = f"{algorithm_key}-python-iteration"
        
        if log_entry.get("improved"):
            old = log_entry.get("oldCost", best_cost)
            new = log_entry.get("newCost", best_cost)
            log_entry["textZh"] = f"Python {label_zh} 第 {log_entry.get('iteration', 0) + 1} 轮刷新最优，代价从 {old:.1f} 降到 {new:.1f}。"
        elif log_entry.get("action") == "generation":
            log_entry["textZh"] = f"Python {label_zh} 第 {log_entry.get('iteration', 0) + 1} 代完成，当前最优 {log_entry.get('cost', best_cost):.1f}，本代耗时约 {log_entry.get('elapsed_ms', 0):.0f} ms。"
        elif log_entry.get("action") == "sa_improved":
            old = log_entry.get("oldCost", best_cost)
            new = log_entry.get("newCost", best_cost)
            log_entry["textZh"] = f"Python {label_zh} 在第 {log_entry.get('iteration', 0) + 1} 轮找到更优解，代价从 {old:.1f} 降到 {new:.1f}，温度 {log_entry.get('temp', 0):.0f}。"
        elif log_entry.get("action") == "sa_restart":
            log_entry["textZh"] = f"Python {label_zh} 退火重启，温度重置。"
        elif log_entry.get("action") == "early_stop":
            log_entry["textZh"] = f"Python {label_zh} 提前收敛，{log_entry.get('reason', '')}。"
        else:
            log_entry["textZh"] = f"Python {label_zh} 第 {log_entry.get('iteration', 0) + 1} 轮：{log_entry.get('action', '迭代')}，当前最优 {log_entry.get('best_cost', best_cost):.1f}，本轮耗时约 {log_entry.get('elapsed_ms', 0):.0f} ms。"
        
        trace_log.append(log_entry)
    
    total_elapsed_ms = (time.perf_counter() - started_at) * 1000.0
    
    improvement = ((initial_cost - best_cost) / max(initial_cost, 0.1)) * 100
    trace_log.append({
        "algorithmKey": algorithm_key,
        "scope": "wave",
        "waveId": wave["waveId"],
        "stage": f"{algorithm_key}-python-finish",
        "initialCost": round(initial_cost, 2),
        "bestCost": round(best_cost, 2),
        "improvement": round(improvement, 2),
        "textZh": "",
        "textJa": "",
    })
    
    return {"bestState": best_state, "traceLog": trace_log}


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




