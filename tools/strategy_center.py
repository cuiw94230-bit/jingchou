"""策略中心：在调用求解器前统一清洗 payload 与约束口径。"""

from copy import deepcopy

SUPPORTED_WAVES = {"W1", "W2", "W3", "W4"}


def _norm_wave_id(value):
    text = str(value or "").strip().upper()
    if text in {"1", "2", "3", "4"}:
        return f"W{text}"
    return text


def _parse_wave_belongs(value):
    text = str(value or "").strip().upper()
    if not text:
        return set()
    text = text.replace("，", ",")
    tokens = [t.strip() for t in text.split(",") if t.strip()]
    parsed = set()
    for token in tokens:
        wave_id = _norm_wave_id(token)
        if wave_id in SUPPORTED_WAVES:
            parsed.add(wave_id)
    return parsed


def _as_float_required(value, field_name, store_id):
    try:
        return float(value)
    except Exception as exc:
        raise ValueError(f"invalid_numeric:{field_name}:{store_id}") from exc


def _norm_store_id(value):
    return str(value or "").strip()


def _sanitize_vehicle_routes(vehicles, allowed_store_ids):
    if not isinstance(vehicles, list):
        return vehicles
    allowed = set(_norm_store_id(v) for v in allowed_store_ids if _norm_store_id(v))
    for vehicle in vehicles:
        if not isinstance(vehicle, dict):
            continue
        routes = vehicle.get("routes")
        if not isinstance(routes, list):
            continue
        cleaned_routes = []
        for route in routes:
            if not isinstance(route, list):
                continue
            cleaned = []
            seen = set()
            for sid in route:
                nsid = _norm_store_id(sid)
                if not nsid or nsid not in allowed or nsid in seen:
                    continue
                seen.add(nsid)
                cleaned.append(nsid)
            if cleaned:
                cleaned_routes.append(cleaned)
        vehicle["routes"] = cleaned_routes
    return vehicles


def apply_strategy_center(payload):
    if not isinstance(payload, dict):
        raise ValueError("payload_must_be_dict")

    data = deepcopy(payload)
    stores = data.get("stores")
    wave = data.get("wave")
    scenario = data.get("scenario")
    dist = data.get("dist")

    if not isinstance(stores, list):
        raise ValueError("stores_required")
    if not isinstance(wave, dict):
        raise ValueError("wave_required")
    if not isinstance(scenario, dict):
        raise ValueError("scenario_required")
    if not isinstance(dist, dict):
        raise ValueError("dist_required")

    wave_id = _norm_wave_id(wave.get("waveId"))
    if wave_id not in SUPPORTED_WAVES:
        raise ValueError(f"unsupported_wave_id:{wave_id}")

    audit = {
        "waveId": wave_id,
        "inputStoreCount": len(stores),
        "filteredZeroLoadStoreIds": [],
        "filteredWaveScopeStoreIds": [],
        "w3KmUnlocked": False,
    }

    kept = []
    for store in stores:
        if not isinstance(store, dict):
            raise ValueError("store_item_must_be_dict")
        store_id = str(store.get("id") or "").strip()
        if not store_id:
            raise ValueError("store_id_required")
        if "waveBelongs" not in store:
            raise ValueError(f"waveBelongs_missing:{store_id}")
        if "boxes" not in store:
            raise ValueError(f"boxes_missing:{store_id}")

        belongs = _parse_wave_belongs(store.get("waveBelongs"))
        if wave_id not in belongs:
            audit["filteredWaveScopeStoreIds"].append(store_id)
            continue

        boxes = _as_float_required(store.get("boxes"), "boxes", store_id)
        if boxes <= 0:
            audit["filteredZeroLoadStoreIds"].append(store_id)
            continue

        kept.append(store)

    data["stores"] = kept
    kept_ids = set(_norm_store_id(item.get("id")) for item in kept if _norm_store_id(item.get("id")))
    data["vehicles"] = _sanitize_vehicle_routes(data.get("vehicles"), kept_ids)
    if isinstance(data.get("wave"), dict) and isinstance(data["wave"].get("storeList"), list):
        data["wave"]["storeList"] = [
            _norm_store_id(sid)
            for sid in data["wave"]["storeList"]
            if _norm_store_id(sid) in kept_ids
        ]

    audit["outputStoreCount"] = len(kept)
    return data, audit
