"""求解器统一运行入口：负责数值规范化、算法分发和结果统一。"""

from decimal import Decimal

# 导入新拆分的算法模块
from .algorithms import run_algorithm, solve_ga
from .vehicle_driven import solve_vehicle_driven

def _normalize_numeric_types(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, dict):
        return {k: _normalize_numeric_types(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_normalize_numeric_types(v) for v in value]
    return value


def run_wave_optimize(payload):
    normalized_payload = _normalize_numeric_types(payload)
    algorithm_key = str(normalized_payload.get("algorithmKey") or "").strip().lower()
    
    # 调试输出：确认算法键和调用分支
    print(f"[RUNNER] algorithm_key = '{algorithm_key}'", flush=True)
    
    if algorithm_key in ("vehicle", "vehicle_driven"):
        print(f"[RUNNER] 调用 vehicle 算法: solve_vehicle_driven", flush=True)
        return solve_vehicle_driven(normalized_payload)
    
    print(f"[RUNNER] 调用通用算法路由: run_algorithm", flush=True)
    return run_algorithm(normalized_payload)


def run_ga_optimize(payload):
    normalized_payload = _normalize_numeric_types(payload)
    print(f"[RUNNER] run_ga_optimize called", flush=True)
    return solve_ga(normalized_payload)