"""线路波次辅助工具：按路线编号差值在同车同日内推断波次标签。"""

import math
# IMPORTANT:
# This function is ONLY valid within the scope of:
# - same delivery_date
# - same vehicle_id
# Do NOT use across multiple vehicles or multiple dates.
def labelRouteWaveByDiff100(routeIds):
    nums = []
    for x in (routeIds or []):
        try:
            v = float(x)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(v):
            continue
        nums.append(int(v) if float(v).is_integer() else v)
    ids = sorted(set(nums))
    used = set()
    result = {}

    for route_id in ids:
        result[route_id] = "single"

    for route_id in ids:
        if route_id in used:
            continue
        pair = route_id + 100
        if pair not in used and pair in result:
            result[route_id] = "first"
            result[pair] = "second"
            used.add(route_id)
            used.add(pair)

    return result
