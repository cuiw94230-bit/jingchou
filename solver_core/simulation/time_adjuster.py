"""推演时间调整小工具：负责分钟与 HH:MM 转换及建议时间生成。"""


def _coerce_minutes(value):
    try:
        return max(0, int(round(float(value))))
    except Exception:
        return 0

def hhmm_to_minutes(text):
    raw = str(text or "").strip()
    if not raw or ":" not in raw:
        return None
    try:
        hour_text, minute_text = raw.split(":", 1)
        hour = int(hour_text)
        minute = int(minute_text)
    except Exception:
        return None
    return hour * 60 + minute


def minutes_to_hhmm(value):
    total = _coerce_minutes(value) % (24 * 60)
    hour = total // 60
    minute = total % 60
    return f"{hour:02d}:{minute:02d}"


def suggest_time(current_time, late_minutes, buffer_minutes=5):
    current_min = hhmm_to_minutes(current_time)
    if current_min is None:
        return None
    suggested = current_min + _coerce_minutes(late_minutes) + _coerce_minutes(buffer_minutes)
    return minutes_to_hhmm(suggested)
