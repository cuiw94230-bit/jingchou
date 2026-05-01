"""Clark-Wright 节约法包装入口。"""

from .constructive_common import solve_constructive

def solve(payload):
    return solve_constructive(payload, "savings")
