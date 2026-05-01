# 算法分发入口：把 algorithmKey 映射到具体求解函数。

from .aco import solve as solve_aco
from .ga import solve as solve_ga
from .hybrid import solve as solve_hybrid
from .lns import solve as solve_lns
from .pso import solve as solve_pso
from .sa import solve as solve_sa
from .savings import solve as solve_savings
from .tabu import solve as solve_tabu
from .vrptw import solve as solve_vrptw


ALGORITHM_SOLVERS = {
    "vrptw": solve_vrptw,
    "savings": solve_savings,
    "hybrid": solve_hybrid,
    "tabu": solve_tabu,
    "lns": solve_lns,
    "sa": solve_sa,
    "aco": solve_aco,
    "pso": solve_pso,
    "ga": solve_ga,
}


def run_algorithm(payload):
    algorithm_key = str(payload.get("algorithmKey") or "tabu").strip().lower()
    solver = ALGORITHM_SOLVERS.get(algorithm_key)
    if solver is None:
        solver = solve_tabu
    return solver(payload)