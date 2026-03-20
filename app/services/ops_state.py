from pathlib import Path
from flask import current_app
from datetime import datetime, timedelta
import json

def _ops_dir():
    path = Path(current_app.instance_path) / "ops"
    path.mkdir(parents=True, exist_ok=True)
    return path

def _safe_day(day):
    return "tomorrow" if str(day).strip().lower() == "tomorrow" else "today"

def _date_key(day="today"):
    now = datetime.now()
    base = now.date()
    if _safe_day(day) == "tomorrow":
        base = base + timedelta(days=1)
    return base.strftime("%Y-%m-%d")

def _state_file(day="today"):
    return _ops_dir() / f"ops_state_{_date_key(day)}.json"

def _default_state(day="today"):
    return {
        "day": _safe_day(day),
        "date": _date_key(day),
        "whole_day_cancelled": False,
        "money": {"ticket_price": 299, "income": 0, "refunds": 0, "net": 0},
        "stats": {"orders_total": 0, "orders_cancelled": 0, "passengers_total": 0, "passengers_cancelled": 0, "flights_total": 0, "flights_cancelled": 0, "flights_tomorrow": 0},
        "flights": {},
        "orders": {}
    }

def load_ops_state(day="today"):
    path = _state_file(day)
    if not path.exists():
        return _default_state(day)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return _default_state(day)
        return data
    except Exception:
        return _default_state(day)

def save_ops_state(day, data):
    with open(_state_file(day), "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def rebuild_ops_state_from_manifests(day, manifests, ticket_price=299):
    state = _default_state(day)
    state["money"]["ticket_price"] = ticket_price
    flights_total = flights_cancelled = passengers_total = passengers_cancelled = orders_total = orders_cancelled = 0
    for manifest in manifests or []:
        slot = (manifest.get("slot_display_full") or manifest.get("slot") or "").strip()
        if not slot:
            continue
        flights_total += 1
        manifest_cancelled = bool(manifest.get("is_cancelled", False))
        flight_orders = manifest.get("orders") or []
        pax_this_flight = cancelled_pax_this_flight = cancelled_orders_this_flight = 0
        for order in flight_orders:
            order_number = (order.get("order_number") or "").strip()
            pax_total = int(order.get("pax_total") or 0)
            is_cancelled = bool(order.get("is_cancelled", False))
            passengers_total += pax_total
            orders_total += 1
            pax_this_flight += pax_total
            if is_cancelled:
                passengers_cancelled += pax_total
                orders_cancelled += 1
                cancelled_pax_this_flight += pax_total
                cancelled_orders_this_flight += 1
            if order_number:
                state["orders"][order_number] = {
                    "order_number": order_number,
                    "customer_name": order.get("customer_name") or "Customer",
                    "email": order.get("email") or "",
                    "mobile": order.get("mobile") or "",
                    "pickup_location": order.get("pickup_location") or order.get("pickup") or "",
                    "pickup_time": order.get("pickup_time") or "",
                    "flight_slot": slot,
                    "pax_total": pax_total,
                    "is_cancelled": is_cancelled,
                    "refund_value": pax_total * ticket_price if is_cancelled else 0
                }
        if manifest_cancelled or (flight_orders and cancelled_orders_this_flight == len(flight_orders)):
            flights_cancelled += 1
            manifest_cancelled = True
        state["flights"][slot] = {
            "slot": slot,
            "is_cancelled": manifest_cancelled,
            "total_pax": pax_this_flight,
            "cancelled_pax": cancelled_pax_this_flight,
            "orders_total": len(flight_orders),
            "orders_cancelled": cancelled_orders_this_flight
        }
    whole_day_cancelled = flights_total > 0 and flights_cancelled == flights_total
    income = passengers_total * ticket_price
    refunds = passengers_cancelled * ticket_price
    net = income - refunds
    state["whole_day_cancelled"] = whole_day_cancelled
    state["stats"] = {
        "orders_total": orders_total,
        "orders_cancelled": orders_cancelled,
        "passengers_total": passengers_total,
        "passengers_cancelled": passengers_cancelled,
        "flights_total": flights_total,
        "flights_cancelled": flights_cancelled,
        "flights_tomorrow": flights_total if _safe_day(day) == "tomorrow" else 0
    }
    state["money"] = {"ticket_price": ticket_price, "income": income, "refunds": refunds, "net": net}
    save_ops_state(day, state)
    return state

def mark_order_cancelled(day, order_number):
    state = load_ops_state(day)
    order = state.get("orders", {}).get(order_number)
    if order and not order.get("is_cancelled"):
        order["is_cancelled"] = True
        pax_total = int(order.get("pax_total") or 0)
        ticket_price = int(state.get("money", {}).get("ticket_price", 299))
        order["refund_value"] = pax_total * ticket_price
    save_ops_state(day, state)
    return state

def mark_flight_cancelled(day, slot):
    state = load_ops_state(day)
    if slot in state.get("flights", {}):
        state["flights"][slot]["is_cancelled"] = True
        for order in state.get("orders", {}).values():
            if order.get("flight_slot") == slot:
                order["is_cancelled"] = True
                pax_total = int(order.get("pax_total") or 0)
                ticket_price = int(state.get("money", {}).get("ticket_price", 299))
                order["refund_value"] = pax_total * ticket_price
    save_ops_state(day, state)
    return state

def mark_whole_day_cancelled(day):
    state = load_ops_state(day)
    state["whole_day_cancelled"] = True
    ticket_price = int(state.get("money", {}).get("ticket_price", 299))
    for flight in state.get("flights", {}).values():
        flight["is_cancelled"] = True
    for order in state.get("orders", {}).values():
        order["is_cancelled"] = True
        pax_total = int(order.get("pax_total") or 0)
        order["refund_value"] = pax_total * ticket_price
    save_ops_state(day, state)
    return state

def apply_ops_state_to_manifests(day, manifests):
    state = load_ops_state(day)
    whole_day_cancelled = bool(state.get("whole_day_cancelled", False))
    flight_map = state.get("flights", {})
    order_map = state.get("orders", {})
    for manifest in manifests or []:
        slot = (manifest.get("slot_display_full") or manifest.get("slot") or "").strip()
        manifest["is_cancelled"] = whole_day_cancelled or bool(flight_map.get(slot, {}).get("is_cancelled", False))
        for order in manifest.get("orders", []) or []:
            order_number = (order.get("order_number") or "").strip()
            state_order = order_map.get(order_number, {})
            order["is_cancelled"] = whole_day_cancelled or bool(state_order.get("is_cancelled", order.get("is_cancelled", False)))
            order["email"] = order.get("email") or state_order.get("email") or ""
            order["mobile"] = order.get("mobile") or state_order.get("mobile") or ""
            order["pickup_location"] = order.get("pickup_location") or state_order.get("pickup_location") or order.get("pickup") or ""
            order["pickup_time"] = order.get("pickup_time") or state_order.get("pickup_time") or ""
    return manifests

def dashboard_stats():
    today = load_ops_state("today")
    tomorrow = load_ops_state("tomorrow")
    return {
        "orders_cancelled": int(today.get("stats", {}).get("orders_cancelled", 0)),
        "flights_tomorrow": int(tomorrow.get("stats", {}).get("flights_total", 0)),
        "income": int(today.get("money", {}).get("income", 0)),
        "refunds": int(today.get("money", {}).get("refunds", 0)),
        "net": int(today.get("money", {}).get("net", 0)),
        "today_whole_day_cancelled": bool(today.get("whole_day_cancelled", False))
    }
