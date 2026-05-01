""""混合算法实现：组合多种局部搜索策略。"""

import time

from ..common_ops import clone_state
from ..evaluator import evaluate_state
from ._meta_common import run_meta_algorithm
from .sa import SimulatedAnnealing
from .lns import LargeNeighborhoodSearch
from .tabu import TabuSearch

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


def _run_optimizer(payload, rng, scenario, wave, stores, vehicles, dist, initial_state, trace_log):
    optimizer = HybridOptimizer(stores, dist, wave, scenario, vehicles, rng)
    return optimizer.optimize(initial_state, 60)


def solve(payload):
    return run_meta_algorithm(payload, "hybrid", _run_optimizer)
