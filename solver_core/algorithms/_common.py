"""算法公共小工具：给 payload 补 algorithmKey。"""

def with_algorithm_key(payload, algorithm_key):    updated = dict(payload or {})
    updated["algorithmKey"] = algorithm_key
    return updated
