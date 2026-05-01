"""元启发式算法公共外壳：共享初始解、迭代日志和结果封装逻辑。"""

import time
import random
from ..evaluator import evaluate_state, evaluate_state_details
from .constructive_common import generate_diverse_initial_state


_LABELS = {
    "hybrid": ("混合VRPTW", "ハイブリッドVRPTW"),
    "tabu": ("禁忌搜索", "タブー探索"),
    "lns": ("大邻域搜索", "大近傍探索"),
    "sa": ("模拟退火", "焼きなまし"),
    "aco": ("蚁群算法", "蟻コロニー最適化"),
    "pso": ("粒子群算法", "粒子群最適化"),
}

_INIT_STRATEGY = {
    "sa": "随机顺序",
    "tabu": "随机顺序",
    "lns": "距离优先",
    "aco": "时间窗优先",
    "pso": "完全随机",
    "hybrid": "Clark-Wright节约法",
}


def run_meta_algorithm(payload, algorithm_key, optimizer_runner):
    started_at = time.perf_counter()
    rng = random.Random(payload.get("randomSeed", 203))
    scenario = payload["scenario"]
    wave = payload["wave"]
    stores = {item["id"]: item for item in payload["stores"]}
    vehicles = payload["vehicles"]
    dist = payload["dist"]

    initial_state = generate_diverse_initial_state(payload, algorithm_key, rng)
    initial_cost = evaluate_state(initial_state, stores, dist, wave, scenario) or 0.0
    trace_log = []
    label_zh, _label_ja = _LABELS.get(algorithm_key, ("后台优化", "バックエンド最適化"))

    trace_log.append(
        {
            "algorithmKey": algorithm_key,
            "scope": "wave",
            "waveId": wave["waveId"],
            "stage": f"{algorithm_key}-python-start",
            "initialCost": round(initial_cost, 2),
            "initStrategy": _INIT_STRATEGY.get(algorithm_key, "Clark-Wright节约法"),
            "textZh": "",
            "textJa": "",
        }
    )

    best_state, optimizer_log = optimizer_runner(
        payload=payload,
        rng=rng,
        scenario=scenario,
        wave=wave,
        stores=stores,
        vehicles=vehicles,
        dist=dist,
        initial_state=initial_state,
        trace_log=trace_log,
    )

    best_cost = evaluate_state(best_state, stores, dist, wave, scenario) or initial_cost
    for log_entry in optimizer_log:
        log_entry["algorithmKey"] = algorithm_key
        log_entry["scope"] = "wave"
        log_entry["waveId"] = wave["waveId"]
        log_entry["stage"] = f"{algorithm_key}-python-iteration"
        if log_entry.get("improved"):
            old = log_entry.get("oldCost", best_cost)
            new = log_entry.get("newCost", best_cost)
            log_entry["textZh"] = (
                f"Python {label_zh} 第 {log_entry.get('iteration', 0) + 1} 轮刷新最优，代价从 {old:.1f} 降到 {new:.1f}。"
            )
        elif log_entry.get("action") == "generation":
            log_entry["textZh"] = (
                f"Python {label_zh} 第 {log_entry.get('iteration', 0) + 1} 代完成，当前最优 {log_entry.get('cost', best_cost):.1f}，本代耗时约 {log_entry.get('elapsed_ms', 0):.0f} ms。"
            )
        elif log_entry.get("action") == "sa_improved":
            old = log_entry.get("oldCost", best_cost)
            new = log_entry.get("newCost", best_cost)
            log_entry["textZh"] = (
                f"Python {label_zh} 在第 {log_entry.get('iteration', 0) + 1} 轮找到更优解，代价从 {old:.1f} 降到 {new:.1f}，温度 {log_entry.get('temp', 0):.0f}。"
            )
        elif log_entry.get("action") == "sa_restart":
            log_entry["textZh"] = f"Python {label_zh} 退火重启，温度重置。"
        elif log_entry.get("action") == "early_stop":
            log_entry["textZh"] = f"Python {label_zh} 提前收敛，{log_entry.get('reason', '')}。"
        else:
            log_entry["textZh"] = (
                f"Python {label_zh} 第 {log_entry.get('iteration', 0) + 1} 轮：{log_entry.get('action', '迭代')}，当前最优 {log_entry.get('best_cost', best_cost):.1f}，本轮耗时约 {log_entry.get('elapsed_ms', 0):.0f} ms。"
            )
        trace_log.append(log_entry)

    _ = (time.perf_counter() - started_at) * 1000.0
    improvement = ((initial_cost - best_cost) / max(initial_cost, 0.1)) * 100
    trace_log.append(
        {
            "algorithmKey": algorithm_key,
            "scope": "wave",
            "waveId": wave["waveId"],
            "stage": f"{algorithm_key}-python-finish",
            "initialCost": round(initial_cost, 2),
            "bestCost": round(best_cost, 2),
            "improvement": round(improvement, 2),
            "textZh": "",
            "textJa": "",
        }
    )
    return {"bestState": best_state, "traceLog": trace_log}
