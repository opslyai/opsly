from collections import defaultdict
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import re
import json
import requests
from flask import current_app

BRISBANE_TZ = ZoneInfo("Australia/Brisbane")
UTC_TZ = ZoneInfo("UTC")


def _headers():
    return {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=UTF-8",
    }


def _base_url():
    return current_app.config["REZDY_API_BASE"].rstrip("/")


def _parse_dt(value):
    if not value:
        return None
    for fmt in (
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            if value.endswith("Z"):
                dt = datetime.strptime(value, fmt).replace(tzinfo=UTC_TZ)
                return dt.astimezone(BRISBANE_TZ)
            dt = datetime.strptime(value, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=BRISBANE_TZ)
            else:
                dt = dt.astimezone(BRISBANE_TZ)
            return dt
        except Exception:
            pass
    return None


def _format_time(dt):
    if not dt:
        return "Unknown"
    return dt.strftime("%I:%M %p").lstrip("0")


def _normalize_value(v):
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        return str(v)
    return str(v).strip()


def _scan_pairs(obj, path=""):
    results = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            key_path = f"{path}.{k}" if path else str(k)
            results.append((str(k), v, key_path))
            results.extend(_scan_pairs(v, key_path))
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            key_path = f"{path}[{i}]"
            results.extend(_scan_pairs(item, key_path))
    return results


def _find_first_text(obj, keywords):
    keywords = [k.lower() for k in keywords]
    for key, value, _ in _scan_pairs(obj):
        if any(k in key.lower() for k in keywords):
            if isinstance(value, dict):
                continue
            text = _normalize_value(value)
            if text and text not in ("[]", "{}"):
                return text
    return ""


def _find_all_text(obj, keywords):
    out = []
    keywords = [k.lower() for k in keywords]
    for key, value, _ in _scan_pairs(obj):
        if any(k in key.lower() for k in keywords):
            text = _normalize_value(value)
            if text and text not in ("[]", "{}"):
                out.append(f"{key}: {text}")
    deduped = []
    seen = set()
    for item in out:
        if item not in seen:
            seen.add(item)
            deduped.append(item)
    return deduped


def _clean_pickup_name(location_name):
    if not location_name:
        return "No pickup location"
    if " - " in location_name:
        return location_name.split(" - ", 1)[1].strip()
    return location_name.strip()


def _extract_pickup_object(booking, item):
    candidates = []

    for source in (item, booking):
        if isinstance(source.get("pickupLocation"), dict):
            candidates.append(source.get("pickupLocation"))
        if isinstance(source.get("pickup"), dict):
            candidates.append(source.get("pickup"))

    for candidate in candidates:
        location_name = candidate.get("locationName") or candidate.get("name") or ""
        pickup_time = candidate.get("pickupTime") or ""
        pickup_instructions = candidate.get("pickupInstructions") or ""
        address = candidate.get("address") or ""
        if location_name or pickup_time or pickup_instructions or address:
            dt = _parse_dt(pickup_time)
            return {
                "location_full": location_name or "No pickup location",
                "location_short": _clean_pickup_name(location_name),
                "pickup_datetime_raw": pickup_time or "",
                "pickup_date": dt.strftime("%Y-%m-%d") if dt else "No pickup date",
                "pickup_time": dt.strftime("%H:%M:%S") if dt else "No pickup time",
                "pickup_instructions": pickup_instructions or "No pickup instructions",
                "pickup_address": address or "No address",
            }

    fallback_location = _find_first_text({"booking": booking, "item": item}, ["pickupLocationName", "pickupLocation", "hotel"])
    fallback_time = _find_first_text({"booking": booking, "item": item}, ["pickupTime", "pickuptime"])
    dt = _parse_dt(fallback_time)

    return {
        "location_full": fallback_location or "No pickup location",
        "location_short": _clean_pickup_name(fallback_location) if fallback_location else "No pickup location",
        "pickup_datetime_raw": fallback_time or "",
        "pickup_date": dt.strftime("%Y-%m-%d") if dt else "No pickup date",
        "pickup_time": dt.strftime("%H:%M:%S") if dt else "No pickup time",
        "pickup_instructions": "No pickup instructions",
        "pickup_address": "No address",
    }


def _extract_order_email(booking, item):
    for source in (item, booking):
        val = _find_first_text(source, ["email"])
        if val:
            return val
    return "No email"


def _extract_order_phone(booking, item):
    for source in (item, booking):
        val = _find_first_text(source, ["phone", "mobile", "telephone"])
        if val:
            return val
    return "No phone"


def _extract_quantities(item):
    quantities = []
    for q in item.get("quantities", []) or []:
        label = q.get("optionLabel") or q.get("label") or q.get("name") or "Pax"
        value = q.get("value", 0) or 0
        try:
            value = int(value)
        except Exception:
            value = 0
        quantities.append({"label": str(label), "value": value})
    return quantities


def _sum_pax(quantities):
    return sum(q.get("value", 0) for q in quantities)


def _parse_weight_number(text):
    if text is None:
        return None
    s = str(text).lower().replace("kg", "").strip()
    m = re.search(r"(\d+(?:\.\d+)?)", s)
    if not m:
        return None
    try:
        return float(m.group(1))
    except Exception:
        return None


def _participant_field_map(participant):
    result = {}
    for field in participant.get("fields", []) or []:
        label = str(field.get("label", "")).strip().lower()
        value = _normalize_value(field.get("value"))
        if label:
            result[label] = value
    return result


def _extract_passengers(booking, item):
    passengers = []
    raw_participants = item.get("participants") or booking.get("participants") or []

    for p in raw_participants:
        fmap = _participant_field_map(p)

        first_name = fmap.get("first name", "")
        last_name = fmap.get("last name", "")
        full_name = f"{first_name} {last_name}".strip() or fmap.get("name") or "Unnamed passenger"

        email = fmap.get("email") or "No email"
        phone = fmap.get("phone") or fmap.get("mobile") or "No phone"

        weight_value = (
            fmap.get("passenger weight")
            or fmap.get("weight")
            or fmap.get("guest weight")
            or ""
        )

        weight_lines = [f"{weight_value} kg"] if weight_value else []
        weight_total = _parse_weight_number(weight_value) if weight_value else None

        passengers.append({
            "name": full_name,
            "email": email,
            "phone": phone,
            "weight_lines": weight_lines,
            "weight_total": weight_total,
            "raw": p,
        })

    if passengers:
        return passengers

    customer = booking.get("customer", {}) or {}
    fallback_name = (
        customer.get("name")
        or f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip()
        or "Unknown customer"
    )

    passengers.append({
        "name": fallback_name,
        "email": _extract_order_email(booking, item),
        "phone": _extract_order_phone(booking, item),
        "weight_lines": _find_all_text({"booking": booking, "item": item}, ["weight"])[:10],
        "weight_total": None,
        "raw": customer,
    })
    return passengers


def _customer_name(booking):
    customer = booking.get("customer", {}) or {}
    return (
        customer.get("name")
        or f"{customer.get('firstName', '')} {customer.get('lastName', '')}".strip()
        or "Unknown customer"
    )


def _bookings_request(params):
    api_key = current_app.config.get("REZDY_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("REZDY_API_KEY is missing from .env")

    merged_params = {"apiKey": api_key, "limit": 100, **(params or {})}

    resp = requests.get(
        f"{_base_url()}/bookings",
        headers=_headers(),
        params=merged_params,
        timeout=45,
    )

    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        raise RuntimeError("Rezdy returned a non-JSON response")

    request_status = data.get("requestStatus", {})
    if resp.status_code >= 400 or not request_status.get("success", False):
        error = request_status.get("error", {})
        raise RuntimeError(error.get("errorMessage", f"Rezdy request failed with status {resp.status_code}"))

    return data


def _iter_bookings(params):
    offset = 0
    while True:
        data = _bookings_request({**(params or {}), "offset": offset})
        bookings = data.get("bookings", []) or []
        for booking in bookings:
            yield booking
        if len(bookings) < 100:
            break
        offset += 100




def _booking_start_datetimes(booking):
    out = []
    for item in booking.get("items", []) or []:
        dt = _parse_dt(item.get("startTimeLocal") or item.get("startTime") or booking.get("startTimeLocal") or booking.get("startTime"))
        if dt:
            out.append(dt)
    if not out:
        dt = _parse_dt(booking.get("startTimeLocal") or booking.get("startTime"))
        if dt:
            out.append(dt)
    return out


def _filter_bookings_by_start_window(bookings, start_local, end_local):
    filtered = []
    for booking in bookings or []:
        starts = _booking_start_datetimes(booking)
        if any(start_local <= dt <= end_local for dt in starts):
            filtered.append(booking)
    return filtered

def _day_window(day_offset=0):
    now_bris = datetime.now(BRISBANE_TZ)
    target_day = (now_bris + timedelta(days=day_offset)).date()
    start_local = datetime.combine(target_day, datetime.min.time(), tzinfo=BRISBANE_TZ)
    end_local = start_local + timedelta(days=1) - timedelta(seconds=1)
    return start_local, end_local


def get_bookings_created_last_days(days=30, statuses=None):
    days = max(int(days or 0), 1)
    statuses = statuses or ["CONFIRMED", "CANCELLED"]
    now_bris = datetime.now(BRISBANE_TZ)
    start_local = now_bris - timedelta(days=days)
    end_local = now_bris

    deduped = {}
    for status in statuses:
        params = {
            "orderStatus": status,
            "minDateCreated": start_local.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "maxDateCreated": end_local.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for booking in _iter_bookings(params):
            order_number = booking.get("orderNumber") or f"booking-{len(deduped)+1}"
            deduped[order_number] = booking
    rows = list(deduped.values())
    rows.sort(key=lambda b: b.get("dateCreated") or "")
    return rows


def _safe_int(value, default=0):
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            return default


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def _extract_amount(booking, item):
    for candidate in (
        item.get("amount"),
        item.get("subtotal"),
        item.get("totalAmount"),
        booking.get("totalAmount"),
    ):
        if candidate not in (None, ""):
            amount = _safe_float(candidate, default=0.0)
            if amount:
                return amount
    option_total = 0.0
    for quantity in item.get("quantities", []) or []:
        option_total += _safe_float(quantity.get("optionPrice"), 0.0) * _safe_int(quantity.get("value"), 0)
    return option_total


def _extract_pickup_name(booking, item):
    pickup = _extract_pickup_object(booking, item)
    name = pickup.get("location_short") or pickup.get("location_full") or "No pickup location"
    return str(name).strip() or "No pickup location"


def _extract_source_name(booking):
    candidates = [
        booking.get("resellerName"),
        booking.get("resellerAlias"),
        booking.get("sourceChannel"),
        booking.get("source"),
        booking.get("resellerSource"),
    ]
    for candidate in candidates:
        text = _normalize_value(candidate)
        if text:
            lowered = text.lower()
            if "getyourguide" in lowered or lowered == "gyg":
                return "GetYourGuide"
            if "viator" in lowered:
                return "Viator"
            if "tripadvisor" in lowered:
                return "Tripadvisor"
            if "direct" in lowered:
                return "Direct"
            return text
    return "Unknown"


def _aggregate_booking_stats(bookings, days=30, date_mode="created"):
    by_source = defaultdict(lambda: {"bookings": 0, "passengers": 0, "revenue": 0.0})
    by_flight = defaultdict(lambda: {"bookings": 0, "passengers": 0, "revenue": 0.0})
    by_day = defaultdict(lambda: {"bookings": 0, "passengers": 0, "revenue": 0.0, "cancelled_bookings": 0, "cancelled_passengers": 0, "cancelled_revenue": 0.0})
    by_pickup = defaultdict(lambda: {"bookings": 0, "passengers": 0})

    totals = {
        "total_bookings": 0,
        "confirmed_bookings": 0,
        "cancelled_bookings": 0,
        "confirmed_passengers": 0,
        "cancelled_passengers": 0,
        "revenue": 0.0,
        "cancelled_revenue": 0.0,
        "missing_contacts": 0,
    }

    for booking in bookings:
        status = _normalize_value(booking.get("status") or "CONFIRMED").upper() or "CONFIRMED"
        is_cancelled = status == "CANCELLED"
        source = _extract_source_name(booking)
        ref_dt = None
        if date_mode == "start":
            ref_dt = _parse_dt(booking.get("startTimeLocal") or booking.get("startTime"))
        if not ref_dt:
            ref_dt = _parse_dt(booking.get("dateCreated")) or _parse_dt(booking.get("dateConfirmed"))
        day_key = ref_dt.strftime("%Y-%m-%d") if ref_dt else "Unknown"

        booking_pax = 0
        booking_revenue = 0.0
        items = booking.get("items", []) or [{}]

        for idx, item in enumerate(items):
            quantities = _extract_quantities(item)
            pax = _sum_pax(quantities)
            amount = _extract_amount(booking, item)
            slot_dt = _parse_dt(item.get("startTimeLocal") or booking.get("startTimeLocal") or item.get("startTime") or booking.get("startTime"))
            slot_label = _format_time(slot_dt)

            booking_pax += pax
            booking_revenue += amount

            if not is_cancelled:
                by_source[source]["bookings"] += 1 if idx == 0 else 0
                by_source[source]["passengers"] += pax
                by_source[source]["revenue"] += amount

                by_flight[slot_label]["bookings"] += 1 if idx == 0 else 0
                by_flight[slot_label]["passengers"] += pax
                by_flight[slot_label]["revenue"] += amount
                pickup_name = _extract_pickup_name(booking, item)
                by_pickup[pickup_name]["bookings"] += 1 if idx == 0 else 0
                by_pickup[pickup_name]["passengers"] += pax

        order_email = _extract_order_email(booking, items[0] if items else {})
        if not (order_email and order_email != "No email"):
            totals["missing_contacts"] += 1

        totals["total_bookings"] += 1
        if is_cancelled:
            totals["cancelled_bookings"] += 1
            totals["cancelled_passengers"] += booking_pax
            totals["cancelled_revenue"] += booking_revenue
            by_day[day_key]["cancelled_bookings"] += 1
            by_day[day_key]["cancelled_passengers"] += booking_pax
            by_day[day_key]["cancelled_revenue"] += booking_revenue
        else:
            totals["confirmed_bookings"] += 1
            totals["confirmed_passengers"] += booking_pax
            totals["revenue"] += booking_revenue
            by_day[day_key]["bookings"] += 1
            by_day[day_key]["passengers"] += booking_pax
            by_day[day_key]["revenue"] += booking_revenue

    source_rows = [{"source": k, **v} for k, v in by_source.items()]
    source_rows.sort(key=lambda x: (-x["revenue"], -x["bookings"], x["source"]))

    flight_rows = [{"slot": k, **v} for k, v in by_flight.items()]
    flight_rows.sort(key=lambda x: (_parse_dt(f"2000-01-01 {x['slot']}") or datetime.max.replace(tzinfo=BRISBANE_TZ), x['slot']))

    pickup_rows = [{"pickup": k, **v} for k, v in by_pickup.items()]
    pickup_rows.sort(key=lambda x: (-x["passengers"], -x["bookings"], x["pickup"]))

    day_rows = [{"date": k, **v} for k, v in by_day.items() if k != "Unknown"]
    day_rows.sort(key=lambda x: x["date"])

    top_source = source_rows[0]["source"] if source_rows else "Unknown"
    top_flight = flight_rows[0]["slot"] if flight_rows else "Unknown"

    return {
        "generated_at": datetime.now(BRISBANE_TZ).isoformat(),
        "window_days": days,
        "total_bookings_30d": int(totals["total_bookings"]),
        "bookings_30d": int(totals["confirmed_bookings"]),
        "confirmed_orders_30d": int(totals["confirmed_bookings"]),
        "confirmed_passengers_30d": int(totals["confirmed_passengers"]),
        "revenue_30d": round(totals["revenue"]),
        "confirmed_revenue_30d": round(totals["revenue"]),
        "cancelled_orders_30d": int(totals["cancelled_bookings"]),
        "cancelled_passengers_30d": int(totals["cancelled_passengers"]),
        "cancelled_revenue_30d": round(totals["cancelled_revenue"]),
        "missing_contacts": int(totals["missing_contacts"]),
        "top_source": top_source,
        "top_flight": top_flight,
        "source_breakdown": [
            {
                "source": row["source"],
                "bookings": int(row["bookings"]),
                "passengers": int(row["passengers"]),
                "revenue": round(row["revenue"]),
            }
            for row in source_rows
        ],
        "flight_breakdown": [
            {
                "slot": row["slot"],
                "bookings": int(row["bookings"]),
                "passengers": int(row["passengers"]),
                "revenue": round(row["revenue"]),
            }
            for row in flight_rows
        ],
        "pickup_breakdown": [
            {
                "pickup": row["pickup"],
                "bookings": int(row["bookings"]),
                "passengers": int(row["passengers"]),
            }
            for row in pickup_rows
        ],
        "bookings_history": [
            {
                "date": row["date"],
                "bookings": int(row["bookings"]),
                "passengers": int(row["passengers"]),
                "revenue": round(row["revenue"]),
                "cancelled_bookings": int(row.get("cancelled_bookings") or 0),
                "cancelled_passengers": int(row.get("cancelled_passengers") or 0),
                "cancelled_revenue": round(row.get("cancelled_revenue") or 0),
            }
            for row in day_rows
        ],
    }


def get_recent_booking_stats(days=30):
    days = max(int(days or 0), 1)
    bookings = get_bookings_created_last_days(days=days)
    return _aggregate_booking_stats(bookings, days=days, date_mode="created")


def get_upcoming_booking_stats(days=30):
    days = max(int(days or 0), 1)
    now_bris = datetime.now(BRISBANE_TZ)
    start_local = now_bris
    end_local = now_bris + timedelta(days=days)

    deduped = {}
    for status in ("CONFIRMED", "CANCELLED"):
        params = {
            "orderStatus": status,
            "minTourStartTime": start_local.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "maxTourStartTime": end_local.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        for booking in _iter_bookings(params):
            order_number = booking.get("orderNumber") or f"booking-{len(deduped)+1}"
            deduped[order_number] = booking

    strict_bookings = _filter_bookings_by_start_window(list(deduped.values()), start_local, end_local)
    payload = _aggregate_booking_stats(strict_bookings, days=days, date_mode="start")
    return {
        "generated_at": payload.get("generated_at"),
        "window_days": payload.get("window_days"),
        "future_bookings_30d": payload.get("confirmed_orders_30d", 0),
        "future_confirmed_orders_30d": payload.get("confirmed_orders_30d", 0),
        "future_cancelled_orders_30d": payload.get("cancelled_orders_30d", 0),
        "future_passengers_30d": payload.get("confirmed_passengers_30d", 0),
        "future_revenue_30d": payload.get("confirmed_revenue_30d", 0),
        "future_cancelled_revenue_30d": payload.get("cancelled_revenue_30d", 0),
        "future_source_breakdown": payload.get("source_breakdown", []),
        "future_flight_breakdown": payload.get("flight_breakdown", []),
        "future_pickup_breakdown": payload.get("pickup_breakdown", []),
        "future_bookings_history": payload.get("bookings_history", []),
    }


def get_bookings_for_day(day_offset=0):
    start_local, end_local = _day_window(day_offset=day_offset)
    bookings = list(_iter_bookings({
        "orderStatus": "CONFIRMED",
        "minTourStartTime": start_local.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "maxTourStartTime": end_local.astimezone(UTC_TZ).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }))
    grouped = defaultdict(list)
    now = datetime.now(BRISBANE_TZ)

    for booking in bookings:
        for item in booking.get("items", []) or []:
            start_raw = item.get("startTimeLocal") or booking.get("startTimeLocal")
            slot_dt = _parse_dt(start_raw)
            slot_key = slot_dt.isoformat() if slot_dt else "unknown"

            pickup = _extract_pickup_object(booking, item)
            quantities = _extract_quantities(item)
            passengers = _extract_passengers(booking, item)

            order_weight_total = 0.0
            any_weight = False
            for p in passengers:
                if p.get("weight_total") is not None:
                    order_weight_total += float(p["weight_total"])
                    any_weight = True

            grouped[slot_key].append({
                "order_number": booking.get("orderNumber", ""),
                "status": booking.get("status", ""),
                "customer_name": _customer_name(booking),
                "product_name": item.get("productName", "") or booking.get("productName", ""),
                "product_code": item.get("productCode", "") or booking.get("productCode", ""),
                "start_time_local": start_raw or "",
                "end_time_local": item.get("endTimeLocal", "") or booking.get("endTimeLocal", ""),
                "pickup": pickup,
                "order_email": _extract_order_email(booking, item),
                "order_phone": _extract_order_phone(booking, item),
                "quantities": quantities,
                "pax_total": _sum_pax(quantities),
                "passengers": passengers,
                "order_weight_total": round(order_weight_total, 1) if any_weight else None,
                "slot_label": _format_time(slot_dt),
                "slot_iso": slot_dt.isoformat() if slot_dt else "",
                "raw_booking_json": json.dumps(booking, indent=2, default=str),
                "raw_item_json": json.dumps(item, indent=2, default=str),
            })

    manifests = []

    for slot_key in sorted(grouped.keys()):
        orders = grouped[slot_key]
        slot_dt = _parse_dt(orders[0].get("start_time_local")) if orders else None

        total_pax = sum(o.get("pax_total", 0) for o in orders)

        total_weight = 0.0
        has_any_weight = False
        pickup_summary = []
        seen_pickups = set()

        for order in orders:
            if order.get("order_weight_total") is not None:
                total_weight += float(order["order_weight_total"])
                has_any_weight = True

            pickup_key = f"{order['pickup']['location_short']}|{order['pickup']['pickup_date']}|{order['pickup']['pickup_time']}"
            if pickup_key not in seen_pickups:
                seen_pickups.add(pickup_key)
                pickup_summary.append({
                    "location": order["pickup"]["location_short"],
                    "date": order["pickup"]["pickup_date"],
                    "time": order["pickup"]["pickup_time"],
                })

        manifests.append({
            "slot": orders[0].get("slot_label", "Unknown"),
            "slot_iso": slot_dt.isoformat() if slot_dt else "",
            "slot_display_full": slot_dt.strftime("%A %d %B %Y · %I:%M %p").lstrip("0") if slot_dt else "Unknown time",
            "departed": bool(slot_dt and now > slot_dt),
            "total_pax": total_pax,
            "total_weight": round(total_weight, 1) if has_any_weight else None,
            "pickup_summary": pickup_summary,
            "orders": orders,
        })

    return manifests


def get_order_detail(order_number, day_offset=0):
    manifests = get_bookings_for_day(day_offset=day_offset)
    for manifest in manifests:
        for order in manifest["orders"]:
            if order.get("order_number") == order_number:
                detail = dict(order)
                detail["flight_slot"] = manifest["slot"]
                detail["flight_slot_full"] = manifest["slot_display_full"]
                detail["flight_total_pax"] = manifest["total_pax"]
                detail["flight_total_weight"] = manifest["total_weight"]
                return detail
    return None


def get_booking_by_order_number(order_number):
    api_key = current_app.config.get("REZDY_API_KEY", "").strip()
    if not api_key or not order_number:
        return None
    resp = requests.get(
        f"{_base_url()}/bookings/{order_number}",
        headers={**_headers(), "apiKey": api_key},
        params={"apiKey": api_key},
        timeout=45,
    )
    try:
        data = resp.json()
    except Exception:
        resp.raise_for_status()
        return None
    request_status = data.get("requestStatus", {}) if isinstance(data, dict) else {}
    if resp.status_code >= 400 or not request_status.get("success", False):
        return None
    return (data.get("booking") if isinstance(data, dict) else None) or None
