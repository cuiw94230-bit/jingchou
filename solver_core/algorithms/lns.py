"""大邻域搜索（LNS）实现。"""

import math
import time
from ..common_ops import (
    clone_state,
    normalize_routes,
    greedy_insert_missing,
    score_for_savings_order,
)
from ..constraints import check_plan_constraints
from ..objective import score_plan_components
from ..evaluator import evaluate_state
from ._meta_common import run_meta_algorithm

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


def _run_optimizer(payload, rng, scenario, wave, stores, vehicles, dist, initial_state, trace_log):
    optimizer = LargeNeighborhoodSearch(stores, dist, wave, scenario, vehicles, rng)
    return optimizer.optimize(initial_state, 60)


def solve(payload):
    return run_meta_algorithm(payload, "lns", _run_optimizer)
