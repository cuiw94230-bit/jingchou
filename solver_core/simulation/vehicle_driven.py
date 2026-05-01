"""推演层对车辆驱动失败分析的轻包装入口。"""

from .failure_analyzer import analyze_wave_failures

def build_failure_records(batch_id, wave_id, payload, solve_result):
    return analyze_wave_failures(batch_id, wave_id, payload, solve_result)
