import json
from pathlib import Path
from datetime import datetime

STATE_FILE = Path("instance/cancellation_state.json")

def _load():
    if not STATE_FILE.exists():
        return {}
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {}

def _save(data):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(data, indent=2))

def cancel_order(day_key, slot, order_number, customer_name, reason):
    data = _load()
    day = data.setdefault(day_key, {"orders": {}, "flights": {}, "day_cancelled": False, "day_reason": None})
    day["orders"][order_number] = {
        "slot": slot,
        "customer_name": customer_name,
        "reason": reason,
        "cancelled_at": datetime.utcnow().isoformat()
    }
    _save(data)

def cancel_flight(day_key, slot, orders, reason):
    data = _load()
    day = data.setdefault(day_key, {"orders": {}, "flights": {}, "day_cancelled": False, "day_reason": None})
    day["flights"][slot] = {
        "reason": reason,
        "cancelled_at": datetime.utcnow().isoformat()
    }
    for order in orders:
        order_number = order.get("order_number", "")
        if order_number:
            day["orders"][order_number] = {
                "slot": slot,
                "customer_name": order.get("customer_name", "Customer"),
                "reason": reason,
                "cancelled_at": datetime.utcnow().isoformat()
            }
    _save(data)

def cancel_day(day_key, manifests, reason):
    data = _load()
    day = data.setdefault(day_key, {"orders": {}, "flights": {}, "day_cancelled": False, "day_reason": None})
    day["day_cancelled"] = True
    day["day_reason"] = reason
    for manifest in manifests:
        slot = manifest.get("slot_display_full") or manifest.get("slot")
        day["flights"][slot] = {
            "reason": reason,
            "cancelled_at": datetime.utcnow().isoformat()
        }
        for order in manifest.get("orders", []):
            order_number = order.get("order_number", "")
            if order_number:
                day["orders"][order_number] = {
                    "slot": slot,
                    "customer_name": order.get("customer_name", "Customer"),
                    "reason": reason,
                    "cancelled_at": datetime.utcnow().isoformat()
                }
    _save(data)

def get_day_state(day_key):
    data = _load()
    return data.get(day_key, {"orders": {}, "flights": {}, "day_cancelled": False, "day_reason": None})

def apply_state_to_manifests(manifests, day_key):
    state = get_day_state(day_key)
    flights_cancelled = 0
    passengers_cancelled = 0

    for manifest in manifests:
        slot_full = manifest.get("slot_display_full") or manifest.get("slot")
        manifest_cancelled = state.get("day_cancelled") or slot_full in state.get("flights", {})
        manifest["is_cancelled"] = manifest_cancelled
        manifest["cancel_reason"] = state.get("day_reason") if state.get("day_cancelled") else state.get("flights", {}).get(slot_full, {}).get("reason")
        manifest_cancelled_pax = 0

        for order in manifest.get("orders", []):
            order_cancelled = manifest_cancelled or order.get("order_number") in state.get("orders", {})
            order["is_cancelled"] = order_cancelled
            order["cancel_reason"] = manifest["cancel_reason"] if manifest_cancelled else state.get("orders", {}).get(order.get("order_number", ""), {}).get("reason")
            if order_cancelled:
                pax = int(order.get("pax_total") or 0)
                manifest_cancelled_pax += pax
                passengers_cancelled += pax

        manifest["cancelled_pax"] = manifest_cancelled_pax
        if manifest_cancelled:
            flights_cancelled += 1

    return {
        "flights_cancelled": flights_cancelled,
        "passengers_cancelled": passengers_cancelled
    }
