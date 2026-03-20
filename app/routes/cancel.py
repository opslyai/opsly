from flask import Blueprint, request, jsonify, current_app
from app.services.cancel_email import send_cancellation_email
from app.services.order_lookup import find_customer_by_order
from pathlib import Path
from datetime import datetime
import json
from app.services.ops_state import rebuild_ops_state_from_manifests, apply_ops_state_to_manifests, mark_whole_day_cancelled, mark_flight_cancelled, mark_order_cancelled, load_ops_state, dashboard_stats

cancel_bp = Blueprint("cancel", __name__)

def _cache_dir():
    path = Path(current_app.instance_path) / "cancellation_cache"
    path.mkdir(parents=True, exist_ok=True)
    return path

def _cache_file(day):
    safe_day = "tomorrow" if str(day).strip().lower() == "tomorrow" else "today"
    stamp = datetime.now().strftime("%Y-%m-%d")
    return _cache_dir() / f"{safe_day}_{stamp}.json"

def _load_cache(day):
    path = _cache_file(day)
    if not path.exists():
        return {"day": day, "date": datetime.now().strftime("%Y-%m-%d"), "orders": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return {"day": day, "date": datetime.now().strftime("%Y-%m-%d"), "orders": {}}
        data.setdefault("orders", {})
        return data
    except Exception:
        return {"day": day, "date": datetime.now().strftime("%Y-%m-%d"), "orders": {}}

def _save_cache(day, payload):
    path = _cache_file(day)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, ensure_ascii=False)

def _normalise_order_payload(item):
    return {
        "order_number": (item.get("order_number") or "").strip(),
        "customer_name": (item.get("customer_name") or item.get("name") or "Customer").strip() or "Customer",
        "customer_email": (item.get("customer_email") or item.get("email") or "").strip(),
        "slot": (item.get("slot") or "").strip(),
        "pax_total": item.get("pax_total", 0),
        "is_cancelled": bool(item.get("is_cancelled", False)),
        "cancel_status": (item.get("cancel_status") or "").strip(),
        "cancel_error": (item.get("cancel_error") or "").strip(),
    }

@cancel_bp.route("/cache_customer_data", methods=["POST"])
def cache_customer_data():
    data = request.get_json(silent=True) or {}
    day = (data.get("day") or "today").strip().lower()
    orders = data.get("orders") or []

    cache = _load_cache(day)
    cache_orders = cache.setdefault("orders", {})

    added = 0
    for item in orders:
        normalised = _normalise_order_payload(item)
        order_number = normalised["order_number"]
        if not order_number:
            continue
        cache_orders[order_number] = normalised
        added += 1

    _save_cache(day, cache)
    return jsonify({
        "status": "ok",
        "day": day,
        "cached_orders": len(cache_orders),
        "added_or_updated": added
    })

@cancel_bp.route("/refresh_customer_data", methods=["POST"])
def refresh_customer_data():
    data = request.get_json(silent=True) or {}
    day = (data.get("day") or "today").strip().lower()
    path = _cache_file(day)
    if path.exists():
        path.unlink()
    return jsonify({"status": "ok", "message": f"{day} cache cleared"})

@cancel_bp.route("/cancel_preview", methods=["GET"])
def cancel_preview():
    order_number = (request.args.get("order_number") or "").strip()
    day = (request.args.get("day") or "today").strip().lower()

    if not order_number:
        return jsonify({"status": "error", "message": "Missing order number"}), 400

    cache = _load_cache(day)
    cached_customer = cache.get("orders", {}).get(order_number)
    if cached_customer:
        return jsonify({
            "status": "ok",
            "source": "cache",
            "order_number": cached_customer.get("order_number") or order_number,
            "customer_name": cached_customer.get("customer_name") or "Customer",
            "customer_email": cached_customer.get("customer_email") or "No email found"
        })

    customer = find_customer_by_order(order_number)

    if not customer:
        return jsonify({
            "status": "error",
            "message": f"No booking found for order number {order_number}"
        }), 404

    cached_payload = _normalise_order_payload({
        "order_number": order_number,
        "customer_name": customer.get("name") or "Customer",
        "customer_email": customer.get("email") or "",
    })
    cache.setdefault("orders", {})[order_number] = cached_payload
    _save_cache(day, cache)

    return jsonify({
        "status": "ok",
        "source": "lookup",
        "order_number": order_number,
        "customer_name": customer.get("name") or "Customer",
        "customer_email": customer.get("email") or "No email found"
    })

@cancel_bp.route("/cancel_booking", methods=["POST"])
def cancel_booking():
    data = request.get_json(silent=True) or {}
    order_number = (data.get("order_number") or "").strip()
    cancel_type = (data.get("cancel_type") or "other").strip().lower()
    day = (data.get("day") or "today").strip().lower()

    if not order_number:
        return jsonify({"status": "error", "message": "Missing order number"}), 400

    cache = _load_cache(day)
    cached_customer = cache.get("orders", {}).get(order_number)

    if cached_customer and cached_customer.get("customer_email"):
        customer = {
            "name": cached_customer.get("customer_name") or "Customer",
            "email": cached_customer.get("customer_email"),
        }
    else:
        customer = find_customer_by_order(order_number)

    if not customer:
        return jsonify({
            "status": "error",
            "message": f"No booking found for order number {order_number}"
        }), 404

    if not customer.get("email"):
        return jsonify({
            "status": "error",
            "message": f"Booking found for {order_number} but no customer email was found"
        }), 400

    send_cancellation_email(
        name=customer.get("name") or "Customer",
        email=customer["email"],
        order_number=order_number,
        cancel_type=cancel_type
    )

    cache.setdefault("orders", {})[order_number] = _normalise_order_payload({
        "order_number": order_number,
        "customer_name": customer.get("name") or "Customer",
        "customer_email": customer.get("email") or "",
        "is_cancelled": True,
        "cancel_status": "sent"
    })
    _save_cache(day, cache)

    return jsonify({
        "status": "sent",
        "order_number": order_number,
        "customer_name": customer.get("name") or "Customer",
        "sent_to": customer["email"],
        "cancel_type": cancel_type
    })
