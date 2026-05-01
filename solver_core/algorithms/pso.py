"""粒子群算法（PSO）实现。"""

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

IGNORE_CAPACITY_CONSTRAINT = False

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


def _run_optimizer(payload, rng, scenario, wave, stores, vehicles, dist, initial_state, trace_log):
    optimizer = ParticleSwarmOptimizer(stores, dist, wave, scenario, vehicles, rng)
    try:
        return optimizer.optimize(initial_state, 40)
    except Exception as pso_error:
        trace_log.append({
            "algorithmKey": "pso",
            "scope": "wave",
            "waveId": wave["waveId"],
            "stage": "pso-python-error",
            "error": str(pso_error),
            "textZh": "",
        })
        return initial_state, []


def solve(payload):
    return run_meta_algorithm(payload, "pso", _run_optimizer)
