"""模拟退火实现。"""

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


def _run_optimizer(payload, rng, scenario, wave, stores, vehicles, dist, initial_state, trace_log):
    optimizer = SimulatedAnnealing(stores, dist, wave, scenario, vehicles, rng)
    return optimizer.optimize(initial_state, 600)


def solve(payload):
    return run_meta_algorithm(payload, "sa", _run_optimizer)
