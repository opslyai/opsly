from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from app.services.analytics import (
    cancellations_count,
    current_operation_snapshot,
    flight_time_breakdown,
    get_snapshot_orders,
    query_orders,
    source_breakdown,
)
from app.services.weather import get_airlie_weather


BRISBANE_FMT = "%a %d %b %Y %H:%M"


def _now_label() -> str:
    return datetime.now().strftime(BRISBANE_FMT)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except Exception:
        return 0


def _is_cancelled(order: Dict[str, Any]) -> bool:
    return bool(order.get("cancelled"))


def _iter_passengers(order: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = order.get("raw_booking") or {}
    items = raw.get("items") or []
    passenger_rows: List[Dict[str, Any]] = []
    for item in items:
        participants = item.get("participants") or []
        for participant in participants:
            fields = participant.get("fields") or []
            record: Dict[str, Any] = {}
            for field in fields:
                label = _clean_text(field.get("label")).lower()
                val = field.get("value")
                if label:
                    record[label] = val
            passenger_rows.append(record)
    if passenger_rows:
        return passenger_rows
    return order.get("passengers") or []


def _missing_weight_count(order: Dict[str, Any]) -> int:
    passengers = _iter_passengers(order)
    if not passengers:
        return _safe_int(order.get("pax_total") or 1)
    missing = 0
    for passenger in passengers:
        weight = passenger.get("passenger weight") if isinstance(passenger, dict) else None
        if weight in (None, "", 0, "0"):
            weight = passenger.get("weight") if isinstance(passenger, dict) else None
        if weight in (None, "", 0, "0"):
            lines = passenger.get("weight_lines") if isinstance(passenger, dict) else None
            if not lines:
                missing += 1
    return missing


def _has_contact_gap(order: Dict[str, Any]) -> bool:
    email = _clean_text(order.get("email"))
    mobile = _clean_text(order.get("mobile"))
    return not email and not mobile


def get_live_context() -> Dict[str, Any]:
    snap = current_operation_snapshot()
    weather = get_airlie_weather()
    return {
        "generated_at": _now_label(),
        "summary": snap,
        "weather": weather,
    }


def get_today_ops_brief() -> Dict[str, Any]:
    snap = current_operation_snapshot()
    weather = get_airlie_weather()
    today_orders = [o for o in get_snapshot_orders(include_today_tomorrow=True) if o.get("day_label") == "today"]
    today_confirmed = [o for o in today_orders if not _is_cancelled(o)]
    today_cancelled = [o for o in today_orders if _is_cancelled(o)]

    by_slot: Dict[str, Dict[str, int]] = defaultdict(lambda: {"orders": 0, "pax": 0})
    for order in today_confirmed:
        slot = _clean_text(order.get("slot_time") or "Unknown")
        by_slot[slot]["orders"] += 1
        by_slot[slot]["pax"] += _safe_int(order.get("pax_total"))
    flight_rows = [{"slot": k, **v} for k, v in by_slot.items()]
    flight_rows.sort(key=lambda row: (-row["pax"], -row["orders"], row["slot"]))

    missing_weights = sum(_missing_weight_count(o) for o in today_confirmed)
    contact_gaps = sum(1 for o in today_confirmed if _has_contact_gap(o))
    risks: List[str] = []
    actions: List[str] = []

    if snap.get("today_whole_day_cancelled"):
        risks.append(
            f"Whole day is cancelled. {snap.get('today_cancelled_passengers', 0)} passengers are affected and refunds tracked are ${snap.get('today_refunds', 0):,.0f}."
        )
        actions.append("Work cancellation communications first and keep tomorrow's flights ready to sell.")
        actions.append("Review agent channels with the highest affected passenger counts before individual customer follow-up.")
    else:
        if missing_weights:
            risks.append(f"{missing_weights} passenger weight entries still look incomplete in today's live orders.")
            actions.append("Chase missing weights before final dispatch and seating decisions.")
        if contact_gaps:
            risks.append(f"{contact_gaps} live bookings have no direct contact details captured in the local snapshot.")
            actions.append("Prioritise agent-side contact for bookings with no direct email or mobile.")
        if weather.get("today", {}).get("rain_chance") not in (None, "") and int(weather.get("today", {}).get("rain_chance") or 0) >= 60:
            risks.append(f"Weather risk is elevated today with rain chance at {int(weather['today']['rain_chance'])}%.")
        if not risks:
            risks.append("No critical operational blocker is obvious from the current local snapshot.")
            actions.append("Stay ahead of tomorrow by clearing any remaining admin gaps today.")

    headline = "Today is fully cancelled" if snap.get("today_whole_day_cancelled") else f"{snap.get('today_orders', 0)} live orders are active today"
    summary = (
        f"{headline}. Current live view shows {snap.get('today_passengers', 0)} confirmed passengers, "
        f"{snap.get('today_cancelled_orders', 0)} cancelled orders, and weather reading {weather.get('summary', 'unknown conditions')} "
        f"at {weather.get('updated_label', 'latest check')}"
    )
    return {
        "headline": headline,
        "summary": summary,
        "metrics": [
            {"label": "Orders today", "value": snap.get("today_orders", 0)},
            {"label": "Passengers today", "value": snap.get("today_passengers", 0)},
            {"label": "Cancelled today", "value": snap.get("today_cancelled_orders", 0)},
            {"label": "Refunds today", "value": f"${snap.get('today_refunds', 0):,.0f}"},
        ],
        "flights": flight_rows[:6],
        "risks": risks,
        "actions": actions,
        "weather": weather,
        "generated_at": _now_label(),
    }


def get_tomorrow_risk_brief() -> Dict[str, Any]:
    snap = current_operation_snapshot()
    weather = get_airlie_weather()
    tomorrow_orders = [o for o in get_snapshot_orders(include_today_tomorrow=True) if o.get("day_label") == "tomorrow"]
    confirmed = [o for o in tomorrow_orders if not _is_cancelled(o)]

    grouped: Dict[str, Dict[str, Any]] = defaultdict(lambda: {
        "slot": "Unknown",
        "orders": 0,
        "pax": 0,
        "missing_weights": 0,
        "contact_gaps": 0,
        "revenue": 0,
    })
    for order in confirmed:
        slot = _clean_text(order.get("slot_time") or "Unknown")
        row = grouped[slot]
        row["slot"] = slot
        row["orders"] += 1
        row["pax"] += _safe_int(order.get("pax_total"))
        row["missing_weights"] += _missing_weight_count(order)
        row["contact_gaps"] += 1 if _has_contact_gap(order) else 0
        row["revenue"] += _safe_int(order.get("revenue"))

    rows = list(grouped.values())
    for row in rows:
        risk_score = row["missing_weights"] * 3 + row["contact_gaps"] * 2 + max(row["pax"] - 4, 0)
        if weather.get("tomorrow", {}).get("rain_chance") not in (None, ""):
            rain = int(weather.get("tomorrow", {}).get("rain_chance") or 0)
            if rain >= 60:
                risk_score += 2
        row["risk_score"] = risk_score
        if risk_score >= 8:
            row["risk_band"] = "High"
        elif risk_score >= 4:
            row["risk_band"] = "Medium"
        else:
            row["risk_band"] = "Low"
    rows.sort(key=lambda row: (-row["risk_score"], -row["pax"], row["slot"]))

    risks: List[str] = []
    if snap.get("tomorrow_whole_day_cancelled"):
        risks.append("Tomorrow is marked as fully cancelled in local ops state.")
    elif rows:
        top = rows[0]
        risks.append(
            f"Highest operational risk is {top['slot']} with score {top['risk_score']} from {top['pax']} passengers, "
            f"{top['missing_weights']} missing weights and {top['contact_gaps']} contact gaps."
        )
    else:
        risks.append("No live tomorrow bookings are currently loaded in the local snapshot.")

    if weather.get("tomorrow", {}).get("rain_chance") not in (None, ""):
        risks.append(
            f"Tomorrow weather outlook is {weather.get('tomorrow', {}).get('summary', 'unknown')} with rain chance {int(weather.get('tomorrow', {}).get('rain_chance') or 0)}%."
        )

    return {
        "headline": "Tomorrow risk board",
        "summary": (
            f"Tomorrow currently shows {snap.get('tomorrow_orders', 0)} confirmed orders, {snap.get('tomorrow_passengers', 0)} passengers, "
            f"and projected confirmed revenue of ${snap.get('tomorrow_revenue', 0):,.0f}."
        ),
        "ranked_flights": rows[:8],
        "risks": risks,
        "actions": [
            "Clear missing weights on the highest-risk flight first.",
            "Use agent channels where direct contact details are missing.",
            "Re-check weather and aircraft availability before sending customer-facing comms.",
        ],
        "generated_at": _now_label(),
    }


def get_missing_info_brief(day: Optional[str] = None) -> Dict[str, Any]:
    orders = get_snapshot_orders(include_today_tomorrow=True)
    if day in {"today", "tomorrow"}:
        orders = [o for o in orders if o.get("day_label") == day]
    confirmed = [o for o in orders if not _is_cancelled(o)]
    rows = []
    for order in confirmed:
        missing_weights = _missing_weight_count(order)
        no_contact = _has_contact_gap(order)
        if not missing_weights and not no_contact:
            continue
        rows.append({
            "order_number": order.get("order_number") or "—",
            "customer_name": order.get("customer_name") or "Customer",
            "slot": order.get("slot_time") or "Unknown",
            "day": order.get("day_label") or "snapshot",
            "source": order.get("source") or "Unknown",
            "pax": _safe_int(order.get("pax_total")),
            "missing_weights": missing_weights,
            "missing_contact": no_contact,
        })
    rows.sort(key=lambda row: (-row["missing_weights"], row["missing_contact"] is False, row["day"], row["slot"], row["customer_name"]))
    return {
        "headline": "Missing customer information",
        "summary": f"{len(rows)} live bookings currently need more customer data in the cached snapshot.",
        "rows": rows[:12],
        "counts": {
            "orders": len(rows),
            "missing_weight_pax": sum(int(r["missing_weights"]) for r in rows),
            "missing_contact_orders": sum(1 for r in rows if r["missing_contact"]),
        },
        "generated_at": _now_label(),
    }


def get_commercial_brief(days: int = 30, source: Optional[str] = None) -> Dict[str, Any]:
    orders = query_orders(days=days, source=source, future_only=False)
    confirmed = [o for o in orders if not _is_cancelled(o)]
    cancelled = [o for o in orders if _is_cancelled(o)]
    src_rows = source_breakdown(days=days)
    flight_rows = flight_time_breakdown(days=days)
    src_label = source or "All sources"
    return {
        "headline": f"Commercial view · last {days} days",
        "summary": (
            f"{src_label} delivered {len(confirmed)} confirmed orders, {sum(_safe_int(o.get('pax_total')) for o in confirmed)} passengers, "
            f"and ${sum(_safe_int(o.get('revenue')) for o in confirmed):,.0f} confirmed revenue in the last {days} days."
        ),
        "metrics": [
            {"label": "Confirmed orders", "value": len(confirmed)},
            {"label": "Cancelled orders", "value": len(cancelled)},
            {"label": "Confirmed revenue", "value": f"${sum(_safe_int(o.get('revenue')) for o in confirmed):,.0f}"},
            {"label": "Cancelled value", "value": f"${sum(_safe_int(o.get('revenue')) for o in cancelled):,.0f}"},
        ],
        "top_sources": src_rows[:6],
        "top_flights": flight_rows[:6],
        "generated_at": _now_label(),
    }


def get_cancellation_brief(days: int = 30, source: Optional[str] = None) -> Dict[str, Any]:
    stats = cancellations_count(days=days, source=source)
    orders = [o for o in query_orders(days=days, source=source, future_only=False) if _is_cancelled(o)]
    source_map: Dict[str, Dict[str, int]] = defaultdict(lambda: {"orders": 0, "pax": 0, "revenue": 0})
    for order in orders:
        src = order.get("source") or "Unknown"
        source_map[src]["orders"] += 1
        source_map[src]["pax"] += _safe_int(order.get("pax_total"))
        source_map[src]["revenue"] += _safe_int(order.get("revenue"))
    rows = [{"source": k, **v} for k, v in source_map.items()]
    rows.sort(key=lambda row: (-row["orders"], -row["revenue"], row["source"]))
    return {
        "headline": f"Cancellations · last {days} days",
        "summary": f"There were {stats['orders']} cancelled bookings affecting {stats['passengers']} passengers and ${stats['revenue']:,.0f} in booking value.",
        "metrics": [
            {"label": "Cancelled orders", "value": stats["orders"]},
            {"label": "Cancelled passengers", "value": stats["passengers"]},
            {"label": "Cancelled value", "value": f"${stats['revenue']:,.0f}"},
        ],
        "breakdown": rows[:6],
        "generated_at": _now_label(),
    }


def get_forward_bookings_brief(days: int = 30, source: Optional[str] = None) -> Dict[str, Any]:
    snap = current_operation_snapshot()
    orders = query_orders(days=days, source=source, future_only=True)
    confirmed = [o for o in orders if not _is_cancelled(o)]
    cancelled = [o for o in orders if _is_cancelled(o)]
    grouped: Dict[str, Dict[str, int]] = defaultdict(lambda: {"orders": 0, "pax": 0, "revenue": 0})
    for order in confirmed:
        slot = f"{order.get('flight_date') or 'Unknown'} · {order.get('slot_time') or 'Unknown'}"
        grouped[slot]["orders"] += 1
        grouped[slot]["pax"] += _safe_int(order.get("pax_total"))
        grouped[slot]["revenue"] += _safe_int(order.get("revenue"))
    rows = [{"slot": k, **v} for k, v in grouped.items()]
    rows.sort(key=lambda row: (-row["revenue"], -row["orders"], row["slot"]))

    confirmed_orders = len(confirmed)
    cancelled_orders = len(cancelled)
    confirmed_pax = sum(_safe_int(o.get('pax_total')) for o in confirmed)
    confirmed_revenue = sum(_safe_int(o.get('revenue')) for o in confirmed)

    if days == 30 and source is None and (not confirmed_orders) and snap.get("next_30"):
        next_30 = snap.get("next_30") or {}
        confirmed_orders = _safe_int(next_30.get("confirmed_orders"))
        cancelled_orders = _safe_int(next_30.get("cancelled_orders"))
        confirmed_pax = _safe_int(next_30.get("confirmed_passengers"))
        confirmed_revenue = _safe_int(next_30.get("confirmed_revenue"))
        rows = (snap.get("future_flight_breakdown") or [])[:8]

    return {
        "headline": f"Forward bookings · next {days} days",
        "summary": (
            f"Confirmed forward demand is {confirmed_orders} orders, {confirmed_pax} passengers, "
            f"and ${confirmed_revenue:,.0f} revenue. "
            f"Cancelled forward bookings are tracked separately at {cancelled_orders} orders."
        ),
        "metrics": [
            {"label": "Confirmed orders", "value": confirmed_orders},
            {"label": "Cancelled orders", "value": cancelled_orders},
            {"label": "Confirmed revenue", "value": f"${confirmed_revenue:,.0f}"},
        ],
        "flights": rows[:8],
        "generated_at": _now_label(),
    }


def get_draft_action_brief(query: str) -> Dict[str, Any]:
    text = _clean_text(query)
    return {
        "headline": "Draft action mode",
        "summary": "Olivia can draft the next operational move even before full automation is wired in.",
        "actions": [
            "Draft a weather cancellation email for all affected passengers.",
            "Prepare an agent update for the highest-impact reseller.",
            "List bookings with missing weights and no contact details.",
        ],
        "draft": (
            "Suggested draft instruction: 'Prepare a concise operational email covering affected flights, passenger impact, "
            "refund or rebooking path, and the next update time.'"
            if text else
            "Suggested draft instruction: 'Prepare a concise operational email covering affected flights, passenger impact, refund or rebooking path, and the next update time.'"
        ),
        "generated_at": _now_label(),
    }
