"""算法核验脚本：检查前台算法入口是否真正指向 solver_core 实现。"""`n`nfrom __future__ import annotations

import json
import re
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
APP_JS = PROJECT_ROOT / "app.js"
DESKTOP_REPORT = Path(r"C:\Users\laoj0\Desktop\九算法日志完整性核验.txt")


def locate_function_lines(source: str) -> dict[str, int]:
    pattern = re.compile(r"^\s*(?:async\s+)?function\s+([A-Za-z0-9_]+)\s*\(", flags=re.MULTILINE)
    mapping: dict[str, int] = {}
    for match in pattern.finditer(source):
        mapping[match.group(1)] = source.count("\n", 0, match.start()) + 1
    return mapping


def has_any_token(source: str, tokens: list[str]) -> bool:
    return any(token in source for token in tokens)


def main() -> int:
    source = APP_JS.read_text(encoding="utf-8")
    functions = locate_function_lines(source)
    checks = {
        "VRPTW": {
            "entry": "solveByVRPTW",
            "optimizer": "greedySolve",
            "must_have": [
                "evaluateStoreInsertionChoices",
                "computeRegretPriority",
                "const unrouted = [...stores]",
            ],
            "log_tokens": [
                "greedySolve(scenario, 0, true)",
                "traceLog.push({",
                "vehicleEvaluations",
                "unscheduledStores.push({",
            ],
            "required_stage_any": [
                "chosenPlate",
                "skipped: true",
            ],
        },
        "Hybrid": {
            "entry": "solveByHybrid",
            "optimizer": "optimizeWaveWithHybrid",
            "must_have": [
                "sampleNeighborhood(currentState, scenario, wave, random, 24, true)",
                "Math.exp((currentCost - candidate.cost)",
                "localImproveState(bestState, scenario, wave, random, 3)",
            ],
            "log_tokens": [
                'algorithmKey: "hybrid"',
                'stage: "hybrid-start"',
                'stage: "hybrid-finish"',
            ],
            "required_stage_any": [
                'stage: "hybrid-iteration"',
                'stage: "hybrid-best"',
            ],
        },
        "GA": {
            "entry": "solveByGA",
            "optimizer": "optimizeWaveWithGA",
            "must_have": [
                "const populationSize = isCompareMode ? 6 : 12;",
                "tournamentSelect(population, random, 3)",
                "crossoverRouteStates(parentA.state, parentB.state, scenario, wave, random)",
                "const eliteCount = isCompareMode ? 2 : 3;",
            ],
            "log_tokens": [
                'algorithmKey: "ga"',
                'stage: "ga-start"',
                'stage: "ga-finish"',
                'stage: "ga-profile-summary"',
            ],
            "required_stage_any": [
                'stage: "ga-generation"',
                'stage: "ga-profile-generation"',
                'stage: "ga-early-stop"',
                'stage: "ga-guard-fill"',
            ],
        },
        "Tabu": {
            "entry": "solveByTabu",
            "optimizer": "optimizeWaveWithTabu",
            "must_have": [
                "const tabu = new Map()",
                "sampleNeighborhood(currentState, scenario, wave, random, 28, true)",
                "candidate.meta?.tabuKeys",
                "const aspiration = candidate.cost + 1e-6 < bestCost",
            ],
            "log_tokens": [
                'algorithmKey: "tabu"',
                'stage: "tabu-start"',
                'stage: "tabu-finish"',
            ],
            "required_stage_any": [
                'stage: "tabu-iteration"',
                'stage: "tabu-best"',
            ],
        },
        "LNS": {
            "entry": "solveByLNS",
            "optimizer": "optimizeWaveWithLns",
            "must_have": [
                "destroyRandom(",
                "destroyShaw(",
                "destroyWorst(",
                "lnsRepairGreedy(",
                "lnsRepairRegret(",
                "runLnsIteration(currentState, scenario, wave, random)",
            ],
            "log_tokens": [
                'algorithmKey: "lns"',
                'stage: "lns-start"',
                'stage: "lns-finish"',
            ],
            "required_stage_any": [
                'stage: "lns-iteration"',
                'stage: "lns-best"',
            ],
        },
        "Savings": {
            "entry": "solveBySavings",
            "optimizer": "solveWaveBySavings",
            "must_have": [
                "buildSavingsCandidatesForWave(",
                "buildSavingsMergeOptions(",
                "twoOptRouteIds(",
                "assignSavingsRoutesToPlans(",
            ],
            "log_tokens": [
                'algorithmKey: "savings"',
                'stage: "savings-start"',
                'stage: "savings-finish"',
            ],
            "required_stage_any": [
                'stage: "savings-merge"',
                'stage: "savings-assignment"',
            ],
        },
        "SA": {
            "entry": "solveBySA",
            "optimizer": "optimizeWaveWithSA",
            "must_have": [
                "Math.exp(-delta / Math.max(0.001, temperature))",
                "temperature *= coolingRate",
                "localImproveState(bestState, scenario, wave, random, 4)",
            ],
            "log_tokens": [
                'algorithmKey: "sa"',
                'stage: "sa-start"',
                'stage: "sa-finish"',
            ],
            "required_stage_any": [
                'stage: "sa-iteration"',
                'stage: "sa-best"',
            ],
        },
        "ACO": {
            "entry": "solveByACO",
            "optimizer": "optimizeWaveWithACO",
            "must_have": [
                "buildPheromoneMapForWave(",
                "selectAcoNextStore(",
                "evaporatePheromone(",
                "depositRoutePheromone(",
            ],
            "log_tokens": [
                'algorithmKey: "aco"',
                'stage: "aco-start"',
                'stage: "aco-finish"',
            ],
            "required_stage_any": [
                'stage: "aco-iteration"',
                'stage: "aco-best"',
            ],
        },
        "PSO": {
            "entry": "solveByPSO",
            "optimizer": "optimizeWaveWithPSO",
            "must_have": [
                "decodePsoPriorityState(",
                "particle.bestPosition[d] - particle.position[d]",
                "globalBest.position[d] - particle.position[d]",
                "particle.position[d] = Math.max(0, Math.min(1, particle.position[d] + velocity))",
            ],
            "log_tokens": [
                'algorithmKey: "pso"',
                'stage: "pso-start"',
                'stage: "pso-finish"',
            ],
            "required_stage_any": [
                'stage: "pso-iteration"',
                'stage: "pso-best"',
            ],
        },
    }

    lines: list[str] = []
    lines.append("城市便利店调度系统 - 九算法与日志完整性核验")
    lines.append(f"源码: {APP_JS}")
    lines.append("")

    summary: dict[str, dict[str, object]] = {}
    for name, cfg in checks.items():
        entry = cfg["entry"]
        optimizer = cfg["optimizer"]
        found_tokens = [token for token in cfg["must_have"] if token in source]
        missing_tokens = [token for token in cfg["must_have"] if token not in source]
        found_logs = [token for token in cfg["log_tokens"] if token in source]
        missing_logs = [token for token in cfg["log_tokens"] if token not in source]
        stage_any_ok = has_any_token(source, cfg["required_stage_any"])
        code_ok = entry in functions and optimizer in functions and not missing_tokens
        log_ok = not missing_logs and stage_any_ok
        passed = code_ok and log_ok

        summary[name] = {
            "passed": passed,
            "code_ok": code_ok,
            "log_ok": log_ok,
            "entry_line": functions.get(entry),
            "optimizer_line": functions.get(optimizer),
            "found_count": len(found_tokens),
            "missing_count": len(missing_tokens),
            "found_log_count": len(found_logs),
            "missing_log_count": len(missing_logs),
        }

        lines.append("=" * 72)
        lines.append(f"算法: {name}")
        lines.append(f"入口函数: {entry}  (line {functions.get(entry, 'N/A')})")
        lines.append(f"核心函数: {optimizer}  (line {functions.get(optimizer, 'N/A')})")
        lines.append(f"代码真实性: {'通过' if code_ok else '未通过'}")
        lines.append(f"日志完整性: {'通过' if log_ok else '未通过'}")
        lines.append(f"总结: {'通过' if passed else '未通过'}")
        lines.append("命中特征:")
        for token in found_tokens:
            lines.append(f"  - {token}")
        if missing_tokens:
            lines.append("缺失代码特征:")
            for token in missing_tokens:
                lines.append(f"  - {token}")
        lines.append("命中日志特征:")
        for token in found_logs:
            lines.append(f"  - {token}")
        if missing_logs:
            lines.append("缺失日志特征:")
            for token in missing_logs:
                lines.append(f"  - {token}")
        lines.append(f"阶段日志至少命中一项: {'是' if stage_any_ok else '否'}")
        lines.append("")

    lines.append("=" * 72)
    lines.append("JSON 摘要")
    lines.append(json.dumps(summary, ensure_ascii=False, indent=2))
    lines.append("")
    DESKTOP_REPORT.write_text("\n".join(lines), encoding="utf-8")
    print(str(DESKTOP_REPORT))
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
