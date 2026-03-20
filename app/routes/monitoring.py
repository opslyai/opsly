import json
from flask import Blueprint, render_template, request
from flask_login import login_required

try:
    from app.services.rezdy import get_bookings_for_day, get_order_detail
except Exception:
    get_bookings_for_day = None
    get_order_detail = None

monitoring = Blueprint("monitoring", __name__, url_prefix="/monitoring")

FLIGHT_TIMES = ["08:30", "10:00", "11:30", "13:00", "14:30", "16:00"]

def _safe_json(value):
    try:
        return json.dumps(value, indent=2, ensure_ascii=False, default=str)
    except Exception:
        return str(value)

def _walk(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk(item)

def _extract_email(order_detail):
    if not order_detail:
        return ""

    invalid_values = {"", "no email", "none", "null", "n/a", "-"}

    keys = [
        "order_email",
        "email",
        "customerEmail",
        "customer_email",
        "contactEmail",
        "contact_email",
        "billingEmail",
        "billing_email",
        "leadEmail",
        "lead_email",
    ]

    for node in _walk(order_detail):
        if not isinstance(node, dict):
            continue
        for key in keys:
            value = node.get(key)
            if value is None:
                continue
            value = str(value).strip()
            if value.lower() in invalid_values:
                continue
            if "@" in value:
                return value

    return ""
def _extract_mobile(order):
    invalid_values = {"", "no phone", "no mobile", "none", "null", "n/a", "-"}
    for node in _walk(order or {}):
        if not isinstance(node, dict):
            continue
        for key in ["order_phone","mobile","phone","customer_mobile","customerMobile","contactPhone","contact_phone"]:
            value = node.get(key)
            if value is None:
                continue
            value = str(value).strip()
            if value.lower() in invalid_values:
                continue
            return value
    return ""
def _extract_name(order):
    for node in _walk(order or {}):
        if not isinstance(node, dict):
            continue
        for key in ["customer_name","customerName","order_name","name","full_name","fullName"]:
            value = node.get(key)
            if value:
                return str(value).strip()
    return "Unknown customer"

def _extract_pickup(order):
    pickup = order.get("pickup") if isinstance(order.get("pickup"), dict) else {}
    return {
        "location": pickup.get("location_short") or pickup.get("location") or order.get("pickup_location") or "",
        "time": pickup.get("pickup_time") or order.get("pickup_time") or "",
        "date": pickup.get("pickup_date") or order.get("pickup_date") or "",
    }

def _extract_flight_time(order):
    for key in ["flight_time","time","startTime","start_time","slot","slot_display","slot_display_full"]:
        value = order.get(key)
        if value:
            return str(value).replace(".", ":").strip()
    return ""

def _extract_order_number(order):
    for key in ["order_number","orderNumber","bookingCode","booking_code","rezdy_order_number","order_no"]:
        value = order.get(key)
        if value:
            return str(value).strip()
    return ""

def _extract_passengers(order):
    passengers = order.get("passengers") or []
    email = _extract_email(order)
    mobile = _extract_mobile(order)
    cleaned = []
    for p in passengers:
        if not isinstance(p, dict):
            continue
        cleaned.append({
            "name": p.get("name") or p.get("full_name") or p.get("fullName") or "Passenger",
            "email": p.get("email") or email,
            "mobile": p.get("mobile") or p.get("phone") or mobile,
            "weight": p.get("weight"),
        })
    return cleaned

def _normalise_customer(order):
    pickup = _extract_pickup(order)
    return {
        "customer_name": _extract_name(order),
        "order_number": _extract_order_number(order),
        "status": order.get("status") or "unknown",
        "email": _extract_email(order),
        "mobile": _extract_mobile(order),
        "pickup_location": pickup["location"],
        "pickup_time": pickup["time"],
        "pickup_date": pickup["date"],
        "flight_time": _extract_flight_time(order),
        "passengers": _extract_passengers(order),
        "raw": order,
    }

def _flatten_bookings(bookings):
    flat = []
    for item in bookings or []:
        if isinstance(item, dict) and isinstance(item.get("orders"), list):
            slot = item.get("slot_display_full") or item.get("slot_display") or item.get("time") or item.get("startTime") or ""
            for order in item.get("orders", []):
                if isinstance(order, dict):
                    merged = dict(order)
                    if not merged.get("flight_time"):
                        merged["flight_time"] = slot
                    flat.append(merged)
        elif isinstance(item, dict):
            flat.append(item)
    return flat

def _matches_name(item, query):
    if not query:
        return True
    q = query.lower().strip()
    hay = " ".join([
        str(item.get("customer_name","")),
        str(item.get("name","")),
        str(item.get("order_number","")),
        str(item.get("bookingCode","")),
        str(item.get("booking_code","")),
    ]).lower()
    for p in item.get("passengers", []) or []:
        if isinstance(p, dict):
            hay += " " + str(p.get("name","")).lower()
    return q in hay

def _normalise_time_string(value):
    s = str(value or "").strip().replace(".", ":")
    if not s:
        return ""
    try:
        from datetime import datetime
        for fmt in ("%I:%M %p", "%I %p", "%H:%M", "%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%A %d %B %Y · %H:%M %p"):
            try:
                return datetime.strptime(s, fmt).strftime("%H:%M")
            except Exception:
                pass
        if "AM" in s.upper() or "PM" in s.upper():
            try:
                return datetime.strptime(s.upper(), "%I:%M %p").strftime("%H:%M")
            except Exception:
                pass
        import re
        m = re.search(r'(\d{1,2}):(\d{2})\s*([AP]M)', s, re.I)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2))
            ap = m.group(3).upper()
            if ap == "PM" and hh != 12:
                hh += 12
            if ap == "AM" and hh == 12:
                hh = 0
            return f"{hh:02d}:{mm:02d}"
        m = re.search(r'(\d{1,2}):(\d{2})', s)
        if m:
            return f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"
    except Exception:
        pass
    return s

def _matches_time(item, query):
    if not query:
        return True
    q = _normalise_time_string(query)
    candidates = [
        item.get("flight_time"),
        item.get("time"),
        item.get("startTime"),
        item.get("slot"),
        item.get("slot_display"),
        item.get("slot_display_full"),
        item.get("start_time"),
        item.get("start_time_local"),
    ]
    for c in candidates:
        if _normalise_time_string(c) == q:
            return True
    return False

@monitoring.route("/", methods=["GET"])
@login_required
def index():
    query_type = request.args.get("query_type", "customer").strip().lower()
    day = request.args.get("day", "today").strip().lower()
    flight_time = request.args.get("flight_time", "").strip()
    customer_name = request.args.get("customer_name", "").strip()
    error = None
    raw_data = []
    results = []
    selected = None
    try:
        if get_bookings_for_day is None:
            error = "get_bookings_for_day is not available in app.services.rezdy"
        else:
            day_offset = 1 if day == "tomorrow" else 0
            raw_data = get_bookings_for_day(day_offset=day_offset) or []
            flat = _flatten_bookings(raw_data)
            filtered = []
            for item in flat:
                if flight_time and not _matches_time(item, flight_time):
                    continue
                if customer_name and not _matches_name(item, customer_name):
                    continue
                filtered.append(_normalise_customer(item))
            results = filtered
            if results:
                selected = results[0]
    except Exception as e:
        error = str(e)
    return render_template("monitoring.html", query_type=query_type, day=day, flight_time=flight_time, customer_name=customer_name, flight_times=FLIGHT_TIMES, results=results, selected=selected, raw_json=_safe_json(selected["raw"] if selected else raw_data), error=error)
