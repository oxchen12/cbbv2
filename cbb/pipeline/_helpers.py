"""
Miscellaneous Python helpers.
"""
from typing import Any

JSONPayload = dict[str, Any]

def deep_get(d: dict | Any,
             *keys: str,
             default: Any = None):
    if not isinstance(d, dict):
        return default
    cur = d
    value = default
    for key in keys:
        value = cur.get(key, None)
        if value is None:
            return default
        cur = value
    return value

def deep_pop(d: dict | Any,
             *keys: str,
             default: Any = None):
    if not isinstance(d, dict) or len(keys) == 0:
        return default
    cur = d
    for key in keys[:-1]:
        value = cur.get(key, None)
        if value is None:
            return default
        cur = value
    return cur.pop(keys[-1], None)

def safe_int(s: str) -> int | None:
    try:
        return int(s)
    except (ValueError, TypeError):
        return None
