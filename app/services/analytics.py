from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

from flask import current_app
from app.services.ops_state import load_ops_state

TICKET_PRICE = 299
AIRLIE_LAT = -20.2676
AIRLIE_LON = 148.7169


def _instance_path() -> Path:
    return Path(current_app.instance_path)


def _snapshot_dir() -> Path:
    return _instance_path() / "day_snapshots"


def _ops_dir() -> Path:
    return _instance_path() / "ops"


def _analytics_dir() -> Path:
    path = _instance_path() / "analytics"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _rezdy_stats_cache_path() -> Path:
    return _analytics_dir() / "rezdy_last_30_days.json"


def _rezdy_next_30_cache_path() -> Path:
    return _analytics_dir() / "rezdy_next_30_days.json"




def _clear_path(path: Path) -> None:
    try:
        if path.is_dir():
            for child in path.iterdir():
                _clear_path(child)
            path.rmdir()
        else:
            path.unlink(missing_ok=True)
    except Exception:
        pass


def _today_iso() -> str:
    return datetime.now().date().isoformat()


def _cache_is_current(payload: Dict[str, Any]) -> bool:
    generated = str((payload or {}).get("generated_at") or "")
    return generated[:10] == _today_iso()

def _ops_state_summary(day: str = "today") -> Dict[str, Any]:
    state = load_ops_state(day) or {}
    stats = state.get("stats") or {}
    money = state.get("money") or {}
    return {
        "whole_day_cancelled": bool(state.get("whole_day_cancelled", False)),
        "orders_total": int(stats.get("orders_total") or 0),
        "orders_cancelled": int(stats.get("orders_cancelled") or 0),
        "passengers_total": int(stats.get("passengers_total") or 0),
        "passengers_cancelled": int(stats.get("passengers_cancelled") or 0),
        "flights_total": int(stats.get("flights_total") or 0),
        "flights_cancelled": int(stats.get("flights_cancelled") or 0),
        "income": int(money.get("income") or 0),
        "refunds": int(money.get("refunds") or 0),
        "net": int(money.get("net") or 0),
    }


def _window_caption(prefix: str, metrics: Dict[str, Any]) -> str:
    return (
        f"{prefix}: {metrics.get('confirmed_orders', 0)} confirmed, "
        f"{metrics.get('cancelled_orders', 0)} cancelled, "
        f"{metrics.get('confirmed_passengers', 0)} confirmed pax, "
        f"${int(metrics.get('confirmed_revenue', 0)):,.0f} confirmed revenue."
    )


def _safe_div(numerator: float, denominator: float) -> float:
    if not denominator:
        return 0.0
    return float(numerator) / float(denominator)


def _window_metrics_from_history(rows: List[Dict[str, Any]], days: int, future: bool = False) -> Dict[str, Any]:
    rows = rows or []
    if not rows:
        return {
            "confirmed_orders": 0,
            "cancelled_orders": 0,
            "confirmed_passengers": 0,
            "cancelled_passengers": 0,
            "confirmed_revenue": 0,
            "cancelled_revenue": 0,
            "avg_bookings_per_day": 0.0,
            "avg_passengers_per_day": 0.0,
            "avg_revenue_per_day": 0.0,
            "busiest_day": "—",
            "busiest_day_bookings": 0,
            "strongest_day_revenue": 0,
            "cancellation_rate": 0.0,
        }

    confirmed_orders = sum(int(r.get("bookings") or 0) for r in rows)
    cancelled_orders = sum(int(r.get("cancelled_bookings") or 0) for r in rows)
    confirmed_passengers = sum(int(r.get("passengers") or 0) for r in rows)
    cancelled_passengers = sum(int(r.get("cancelled_passengers") or 0) for r in rows)
    confirmed_revenue = sum(int(r.get("revenue") or 0) for r in rows)
    cancelled_revenue = sum(int(r.get("cancelled_revenue") or 0) for r in rows)

    busiest = max(rows, key=lambda r: (int(r.get("bookings") or 0), int(r.get("passengers") or 0), r.get("date") or ""))
    strongest = max(rows, key=lambda r: (int(r.get("revenue") or 0), int(r.get("bookings") or 0), r.get("date") or ""))

    total_orders = confirmed_orders + cancelled_orders
    return {
        "confirmed_orders": confirmed_orders,
        "cancelled_orders": cancelled_orders,
        "confirmed_passengers": confirmed_passengers,
        "cancelled_passengers": cancelled_passengers,
        "confirmed_revenue": confirmed_revenue,
        "cancelled_revenue": cancelled_revenue,
        "avg_bookings_per_day": round(_safe_div(confirmed_orders, max(days, 1)), 1),
        "avg_passengers_per_day": round(_safe_div(confirmed_passengers, max(days, 1)), 1),
        "avg_revenue_per_day": round(_safe_div(confirmed_revenue, max(days, 1))),
        "busiest_day": busiest.get("date") or "—",
        "busiest_day_bookings": int(busiest.get("bookings") or 0),
        "strongest_day_revenue": int(strongest.get("revenue") or 0),
        "cancellation_rate": round(_safe_div(cancelled_orders * 100.0, total_orders), 1),
    }


def _derive_dashboard_metrics(summary: Dict[str, Any]) -> Dict[str, Any]:
    summary = dict(summary)
    bookings_history = summary.get("bookings_history") or []
    future_history = summary.get("future_bookings_history") or []

    last_30 = _window_metrics_from_history(bookings_history[-30:], 30, future=False)
    last_5 = _window_metrics_from_history(bookings_history[-5:], 5, future=False)
    next_30 = _window_metrics_from_history(future_history[:30], 30, future=True)
    next_5 = _window_metrics_from_history(future_history[:5], 5, future=True)

    summary["last_30"] = last_30
    summary["last_5"] = last_5
    summary["next_30"] = next_30
    summary["next_5"] = next_5

    summary["confirmed_booking_rate_30d"] = round(100 - last_30["cancellation_rate"], 1)
    summary["cancelled_booking_rate_30d"] = last_30["cancellation_rate"]
    summary["confirmed_avg_value_30d"] = round(_safe_div(last_30["confirmed_revenue"], max(last_30["confirmed_orders"], 1)))
    summary["cancelled_avg_value_30d"] = round(_safe_div(last_30["cancelled_revenue"], max(last_30["cancelled_orders"], 1))) if last_30["cancelled_orders"] else 0
    summary["forward_avg_value_30d"] = round(_safe_div(next_30["confirmed_revenue"], max(next_30["confirmed_orders"], 1)))

    source_rows = summary.get("source_breakdown") or []
    future_source_rows = summary.get("future_source_breakdown") or []
    if source_rows:
        total_source_revenue = sum(int(r.get("revenue") or 0) for r in source_rows)
        for row in source_rows:
            row["share_pct"] = round(_safe_div((row.get("revenue") or 0) * 100.0, total_source_revenue), 1)
    if future_source_rows:
        total_future_revenue = sum(int(r.get("revenue") or 0) for r in future_source_rows)
        for row in future_source_rows:
            row["share_pct"] = round(_safe_div((row.get("revenue") or 0) * 100.0, total_future_revenue), 1)

    flight_rows = summary.get("flight_breakdown") or []
    future_flight_rows = summary.get("future_flight_breakdown") or []
    summary["top_source_share_30d"] = source_rows[0].get("share_pct", 0) if source_rows else 0
    summary["top_future_source"] = future_source_rows[0].get("source", "Unknown") if future_source_rows else "Unknown"
    summary["top_future_flight"] = future_flight_rows[0].get("slot", "Unknown") if future_flight_rows else "Unknown"
    summary["top_flight_passengers"] = int(flight_rows[0].get("passengers") or 0) if flight_rows else 0
    summary["top_future_flight_passengers"] = int(future_flight_rows[0].get("passengers") or 0) if future_flight_rows else 0
    return summary


def ensure_daily_rezdy_cache(force: bool = False) -> Dict[str, Any]:
    from app.services.rezdy import get_recent_booking_stats, get_upcoming_booking_stats

    last_cache = load_rezdy_stats_cache("last_30")
    next_cache = load_rezdy_stats_cache("next_30")
    if force or not _cache_is_current(last_cache):
        last_cache = get_recent_booking_stats(days=30)
        save_rezdy_stats_cache(last_cache, window="last_30")
    if force or not _cache_is_current(next_cache):
        next_cache = get_upcoming_booking_stats(days=30)
        save_rezdy_stats_cache(next_cache, window="next_30")
    return {"last_30": last_cache, "next_30": next_cache}

def _safe_json_load(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _parse_json_like(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            return {}
    return {}


def _extract_source(order: Dict[str, Any]) -> str:
    raw = _parse_json_like(order.get("raw_booking_json"))
    candidates = [
        raw.get("resellerName"),
        raw.get("resellerAlias"),
        raw.get("sourceChannel"),
        raw.get("source"),
        raw.get("resellerSource"),
        order.get("agent"),
        order.get("source"),
    ]
    for candidate in candidates:
        if candidate and str(candidate).strip():
            text = str(candidate).strip()
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


def _date_from_string(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    text = str(value).strip()
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
        "%Y-%m-%d",
        "%A %d %B %Y · %I:%M %p",
    ):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            continue
    m = re.search(r"(\d{4}-\d{2}-\d{2})", text)
    if m:
        try:
            return datetime.strptime(m.group(1), "%Y-%m-%d")
        except Exception:
            pass
    return None


def _slot_time(order: Dict[str, Any]) -> str:
    for key in ("slot_label", "slot_display_full", "flight_slot", "start_time_local", "slot"):
        value = order.get(key)
        if value:
            return str(value)
    return "Unknown"

def _slot_sort_key(slot: str) -> tuple:
    raw = str(slot or '').strip()
    if not raw:
        return (99, 99, raw)
    for fmt in ("%I:%M %p", "%H:%M:%S", "%H:%M"):
        try:
            dt = datetime.strptime(raw, fmt)
            return (dt.hour, dt.minute, raw)
        except Exception:
            continue
    m = re.search(r"(\d{1,2}:\d{2}(?::\d{2})?\s*[APMapm]{0,2})", raw)
    if m:
        return _slot_sort_key(m.group(1).upper().replace('AM', ' AM').replace('PM', ' PM').replace('  ', ' ').strip())
    return (99, 99, raw)


def _pickup_name(order: Dict[str, Any]) -> str:
    raw = order.get('raw_booking') or {}
    candidates = [
        order.get('pickup_location'),
        raw.get('pickupLocationName'),
        raw.get('pickupLocation', {}).get('locationName') if isinstance(raw.get('pickupLocation'), dict) else '',
    ]
    for candidate in candidates:
        text = str(candidate or '').strip()
        if text and text.lower() not in {'no pickup location', 'unknown'}:
            return text
    return 'Unassigned'


def _trim_pickup_name(name: str) -> str:
    text = str(name or '').strip()
    if not text:
        return 'Unassigned'
    if ' - ' in text:
        text = text.split(' - ', 1)[1].strip()
    return text


def _pickup_breakdown(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, int]] = defaultdict(lambda: {'bookings': 0, 'passengers': 0})
    for order in orders or []:
        if order.get('cancelled'):
            continue
        pickup = _trim_pickup_name(_pickup_name(order))
        grouped[pickup]['bookings'] += 1
        grouped[pickup]['passengers'] += int(order.get('pax_total') or 0)
    rows = [{'pickup': k, **v} for k, v in grouped.items()]
    rows.sort(key=lambda x: (-x['passengers'], -x['bookings'], x['pickup']))
    return rows


def _top_pickup_sentence(rows: List[Dict[str, Any]], limit: int = 3) -> str:
    top = rows[:limit]
    if not top:
        return 'No pickup hotspots are cached yet.'
    parts = []
    for row in top:
        parts.append(f"{row['pickup']} ({row['passengers']} pax)")
    return ', '.join(parts)


def _today_pickup_rows(orders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return _pickup_breakdown([o for o in orders if o.get('day_label') == 'today'])


def _normalise_order(order: Dict[str, Any], day_label: str, snapshot_date: Optional[str] = None) -> Dict[str, Any]:
    start_dt = _date_from_string(order.get("start_time_local"))
    booking_raw = _parse_json_like(order.get("raw_booking_json"))
    booking_dt = _date_from_string(
        booking_raw.get("createdAt")
        or booking_raw.get("created_at")
        or booking_raw.get("dateCreated")
        or booking_raw.get("created")
    )
    source = _extract_source(order)
    pax_total = int(order.get("pax_total") or 0)
    revenue = pax_total * TICKET_PRICE
    status = str(order.get("status") or booking_raw.get("status") or "").upper() or "CONFIRMED"
    cancelled = status in {"CANCELLED", "CANCELED"} or bool(order.get("is_cancelled"))
    flight_date = (start_dt.date().isoformat() if start_dt else snapshot_date) or ""
    return {
        "order_number": order.get("order_number") or "",
        "customer_name": order.get("customer_name") or "Customer",
        "product_name": order.get("product_name") or "",
        "source": source,
        "status": status,
        "cancelled": cancelled,
        "pax_total": pax_total,
        "revenue": revenue,
        "start_time_local": order.get("start_time_local") or "",
        "start_dt": start_dt,
        "booking_dt": booking_dt,
        "flight_date": flight_date,
        "day_label": day_label,
        "slot_time": _slot_time(order),
        "email": order.get("email") or order.get("order_email") or "",
        "raw_booking": booking_raw,
    }


def get_snapshot_orders(include_today_tomorrow: bool = True) -> List[Dict[str, Any]]:
    orders: List[Dict[str, Any]] = []
    snap_dir = _snapshot_dir()
    if not snap_dir.exists():
        return orders
    for path in sorted(snap_dir.glob("*.json")):
        payload = _safe_json_load(path) or {}
        manifests = payload.get("manifests") or []
        day_label = "today" if "today_" in path.name else "tomorrow" if "tomorrow_" in path.name else "snapshot"
        snapshot_date = payload.get("date")
        if not include_today_tomorrow and day_label in {"today", "tomorrow"}:
            continue
        for manifest in manifests:
            for order in manifest.get("orders") or []:
                orders.append(_normalise_order(order, day_label=day_label, snapshot_date=snapshot_date))
    return orders


def get_historical_daily_stats() -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    ops_dir = _ops_dir()
    if not ops_dir.exists():
        return rows
    for path in sorted(ops_dir.glob("ops_state_*.json")):
        data = _safe_json_load(path) or {}
        stats = data.get("stats") or {}
        money = data.get("money") or {}
        rows.append({
            "date": data.get("date") or path.stem.replace("ops_state_", ""),
            "day": data.get("day") or "today",
            "orders_total": int(stats.get("orders_total") or 0),
            "orders_cancelled": int(stats.get("orders_cancelled") or 0),
            "passengers_total": int(stats.get("passengers_total") or 0),
            "passengers_cancelled": int(stats.get("passengers_cancelled") or 0),
            "flights_total": int(stats.get("flights_total") or 0),
            "flights_cancelled": int(stats.get("flights_cancelled") or 0),
            "income": int(money.get("income") or 0),
            "refunds": int(money.get("refunds") or 0),
            "net": int(money.get("net") or 0),
        })
    return rows


def _days_ago_cutoff(days: int) -> datetime:
    return datetime.now() - timedelta(days=max(int(days), 0))


def load_rezdy_stats_cache(window: str = "last_30") -> Dict[str, Any]:
    path = _rezdy_next_30_cache_path() if window == "next_30" else _rezdy_stats_cache_path()
    return _safe_json_load(path) or {}


def save_rezdy_stats_cache(payload: Dict[str, Any], window: str = "last_30") -> str:
    path = _rezdy_next_30_cache_path() if window == "next_30" else _rezdy_stats_cache_path()
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return str(path)


def clear_rezdy_stats_cache() -> None:
    for path in (_analytics_dir(), _instance_path() / "ops_state", _instance_path() / "cancellation_cache"):
        _clear_path(path)
    _clear_path(_instance_path() / "cancellation_state.json")


def query_orders(days: int = 30, source: Optional[str] = None, future_only: bool = False) -> List[Dict[str, Any]]:
    source_l = (source or "").strip().lower()
    cutoff = _days_ago_cutoff(days)
    now = datetime.now()
    results: List[Dict[str, Any]] = []
    for order in get_snapshot_orders(include_today_tomorrow=True):
        compare_dt = order.get("booking_dt") or order.get("start_dt")
        if future_only:
            start_dt = order.get("start_dt")
            if not start_dt:
                continue
            if not (now <= start_dt <= now + timedelta(days=days)):
                continue
        else:
            if compare_dt and compare_dt.replace(tzinfo=None) < cutoff.replace(tzinfo=None):
                continue
        if source_l and source_l not in (order.get("source") or "").lower():
            continue
        results.append(order)
    return results


def bookings_count(days: int = 30, source: Optional[str] = None) -> int:
    return len(query_orders(days=days, source=source, future_only=False))


def passengers_count(days: int = 30, source: Optional[str] = None) -> int:
    return sum(int(o.get("pax_total") or 0) for o in query_orders(days=days, source=source, future_only=False))


def revenue_count(days: int = 30, source: Optional[str] = None) -> int:
    return sum(int(o.get("revenue") or 0) for o in query_orders(days=days, source=source, future_only=False))


def future_revenue(days_ahead: int = 30, source: Optional[str] = None) -> int:
    return sum(int(o.get("revenue") or 0) for o in query_orders(days=days_ahead, source=source, future_only=True) if not o.get("cancelled"))


def cancellations_count(days: int = 30, source: Optional[str] = None) -> Dict[str, int]:
    orders = [o for o in query_orders(days=days, source=source, future_only=False) if o.get("cancelled")]
    return {
        "orders": len(orders),
        "passengers": sum(int(o.get("pax_total") or 0) for o in orders),
        "revenue": sum(int(o.get("revenue") or 0) for o in orders),
    }


def source_breakdown(days: int = 30) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, int]] = defaultdict(lambda: {"bookings": 0, "passengers": 0, "revenue": 0})
    for order in query_orders(days=days):
        src = order.get("source") or "Unknown"
        grouped[src]["bookings"] += 1
        grouped[src]["passengers"] += int(order.get("pax_total") or 0)
        grouped[src]["revenue"] += int(order.get("revenue") or 0)
    rows = [{"source": k, **v} for k, v in grouped.items()]
    rows.sort(key=lambda x: (-x["revenue"], -x["bookings"], x["source"]))
    return rows


def flight_time_breakdown(days: int = 30) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, int]] = defaultdict(lambda: {"bookings": 0, "passengers": 0, "revenue": 0})
    for order in query_orders(days=days):
        slot = order.get("slot_time") or "Unknown"
        grouped[slot]["bookings"] += 1
        grouped[slot]["passengers"] += int(order.get("pax_total") or 0)
        grouped[slot]["revenue"] += int(order.get("revenue") or 0)
    rows = [{"slot": k, **v} for k, v in grouped.items()]
    rows.sort(key=lambda x: _slot_sort_key(x['slot']))
    return rows


def bookings_by_day(days: int = 30) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, int]] = defaultdict(lambda: {"bookings": 0, "passengers": 0, "revenue": 0})
    cutoff = _days_ago_cutoff(days)
    for row in get_historical_daily_stats():
        dt = _date_from_string(row.get("date"))
        if dt and dt >= cutoff:
            grouped[row["date"]]["bookings"] += int(row.get("orders_total") or 0)
            grouped[row["date"]]["passengers"] += int(row.get("passengers_total") or 0)
            grouped[row["date"]]["revenue"] += int(row.get("income") or 0)
    rows = [{"date": k, **v} for k, v in grouped.items()]
    rows.sort(key=lambda x: x["date"])
    return rows


def _merge_rezdy_summary(summary: Dict[str, Any], rezdy_cache: Dict[str, Any], next_cache: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(summary)

    if rezdy_cache:
        for key in (
            "total_bookings_30d",
            "bookings_30d",
            "confirmed_orders_30d",
            "confirmed_passengers_30d",
            "revenue_30d",
            "confirmed_revenue_30d",
            "cancelled_orders_30d",
            "cancelled_passengers_30d",
            "cancelled_revenue_30d",
            "missing_contacts",
            "top_source",
            "top_flight",
            "source_breakdown",
            "flight_breakdown",
            "bookings_history",
        ):
            if key in rezdy_cache and rezdy_cache.get(key) not in (None, []):
                merged[key] = rezdy_cache.get(key)
        merged["analytics_source"] = "rezdy"
        merged["rezdy_last_synced"] = rezdy_cache.get("generated_at")
    else:
        merged["analytics_source"] = "local"
        merged["rezdy_last_synced"] = None

    if next_cache:
        for key in (
            "future_bookings_30d",
            "future_confirmed_orders_30d",
            "future_cancelled_orders_30d",
            "future_passengers_30d",
            "future_revenue_30d",
            "future_cancelled_revenue_30d",
            "future_source_breakdown",
            "future_flight_breakdown",
            "future_bookings_history",
        ):
            if key in next_cache and next_cache.get(key) not in (None, []):
                merged[key] = next_cache.get(key)
        merged["rezdy_next30_synced"] = next_cache.get("generated_at")
        merged["future_analytics_source"] = "rezdy"
    else:
        merged["rezdy_next30_synced"] = None
        merged["future_analytics_source"] = merged.get("analytics_source", "local")

    return merged


def current_operation_snapshot() -> Dict[str, Any]:
    orders = get_snapshot_orders(include_today_tomorrow=True)
    today_orders = [o for o in orders if o.get("day_label") == "today"]
    tomorrow_orders = [o for o in orders if o.get("day_label") == "tomorrow"]

    today_state = _ops_state_summary("today")
    tomorrow_state = _ops_state_summary("tomorrow")

    all_30_orders = query_orders(30)
    confirmed_30_orders = [o for o in all_30_orders if not o.get("cancelled")]

    sources = source_breakdown(30)
    top_source = sources[0]["source"] if sources else "Unknown"
    flight_breakdown = flight_time_breakdown(30)
    top_flight = flight_breakdown[0]["slot"] if flight_breakdown else "Unknown"

    future_orders = query_orders(days=30, future_only=True)
    future_confirmed_orders = [o for o in future_orders if not o.get("cancelled")]
    future_cancelled_orders = [o for o in future_orders if o.get("cancelled")]

    today_confirmed_orders = [o for o in today_orders if not o.get("cancelled")]
    today_cancelled_orders = [o for o in today_orders if o.get("cancelled")]
    tomorrow_confirmed_orders = [o for o in tomorrow_orders if not o.get("cancelled")]
    tomorrow_cancelled_orders = [o for o in tomorrow_orders if o.get("cancelled")]

    today_pax_raw = sum(int(o.get("pax_total") or 0) for o in today_confirmed_orders)
    tomorrow_pax_raw = sum(int(o.get("pax_total") or 0) for o in tomorrow_confirmed_orders)
    today_revenue = sum(int(o.get("revenue") or 0) for o in today_confirmed_orders)
    tomorrow_revenue = sum(int(o.get("revenue") or 0) for o in tomorrow_confirmed_orders)

    if today_state["whole_day_cancelled"]:
        today_confirmed_orders = []
        today_pax_raw = 0
        today_revenue = 0
    if tomorrow_state["whole_day_cancelled"]:
        tomorrow_confirmed_orders = []
        tomorrow_pax_raw = 0
        tomorrow_revenue = 0

    today_pax = min(today_pax_raw, 35)
    tomorrow_pax = min(tomorrow_pax_raw, 35)

    missing_contact = sum(1 for o in orders if not (o.get("email") or "").strip())
    cancellation_stats = cancellations_count(30)
    history = bookings_by_day(30)
    today_row = history[-1] if history else {"bookings": len(today_orders), "revenue": today_revenue}
    previous_row = history[-2] if len(history) >= 2 else {"bookings": 0, "revenue": 0}
    booking_delta = int(today_row.get("bookings", 0)) - int(previous_row.get("bookings", 0))

    today_pickups = _today_pickup_rows(orders)
    last_pickups = _pickup_breakdown(confirmed_30_orders)
    next_pickups = _pickup_breakdown(future_confirmed_orders)

    snap = {
        "today_orders": 0 if today_state["whole_day_cancelled"] else len(today_confirmed_orders),
        "today_cancelled_orders": max(today_state["orders_cancelled"], len(today_cancelled_orders)),
        "today_passengers": 0 if today_state["whole_day_cancelled"] else today_pax,
        "today_passengers_uncapped": 0 if today_state["whole_day_cancelled"] else today_pax_raw,
        "today_cancelled_passengers": today_state["passengers_cancelled"],
        "today_revenue": 0 if today_state["whole_day_cancelled"] else today_revenue,
        "today_whole_day_cancelled": today_state["whole_day_cancelled"],
        "today_flights_cancelled": today_state["flights_cancelled"],
        "today_flights_total": today_state["flights_total"],
        "today_refunds": today_state["refunds"],
        "tomorrow_orders": 0 if tomorrow_state["whole_day_cancelled"] else len(tomorrow_confirmed_orders),
        "tomorrow_cancelled_orders": max(tomorrow_state["orders_cancelled"], len(tomorrow_cancelled_orders)),
        "tomorrow_passengers": 0 if tomorrow_state["whole_day_cancelled"] else tomorrow_pax,
        "tomorrow_passengers_uncapped": 0 if tomorrow_state["whole_day_cancelled"] else tomorrow_pax_raw,
        "tomorrow_cancelled_passengers": tomorrow_state["passengers_cancelled"],
        "tomorrow_revenue": 0 if tomorrow_state["whole_day_cancelled"] else tomorrow_revenue,
        "tomorrow_whole_day_cancelled": tomorrow_state["whole_day_cancelled"],
        "tomorrow_flights_cancelled": tomorrow_state["flights_cancelled"],
        "tomorrow_flights_total": tomorrow_state["flights_total"],
        "bookings_7d": sum(row.get("bookings", 0) for row in bookings_by_day(7)),
        "total_bookings_30d": len(all_30_orders),
        "bookings_30d": len(confirmed_30_orders),
        "confirmed_orders_30d": len(confirmed_30_orders),
        "confirmed_passengers_30d": sum(int(o.get("pax_total") or 0) for o in confirmed_30_orders),
        "revenue_30d": sum(int(o.get("revenue") or 0) for o in confirmed_30_orders),
        "confirmed_revenue_30d": sum(int(o.get("revenue") or 0) for o in confirmed_30_orders),
        "future_bookings_30d": len(future_confirmed_orders),
        "future_confirmed_orders_30d": len(future_confirmed_orders),
        "future_cancelled_orders_30d": len(future_cancelled_orders),
        "future_passengers_30d": sum(int(o.get("pax_total") or 0) for o in future_confirmed_orders),
        "future_revenue_30d": sum(int(o.get("revenue") or 0) for o in future_confirmed_orders),
        "future_cancelled_revenue_30d": sum(int(o.get("revenue") or 0) for o in future_cancelled_orders),
        "cancelled_orders_30d": cancellation_stats["orders"],
        "cancelled_passengers_30d": cancellation_stats["passengers"],
        "cancelled_revenue_30d": cancellation_stats["revenue"],
        "top_source": top_source,
        "top_flight": top_flight,
        "missing_contacts": missing_contact,
        "source_breakdown": sources,
        "flight_breakdown": flight_breakdown,
        "bookings_history": history,
        "booking_delta": booking_delta,
        "today_pickup_breakdown": today_pickups,
        "pickup_breakdown": last_pickups,
        "future_pickup_breakdown": next_pickups,
    }
    merged = _merge_rezdy_summary(snap, load_rezdy_stats_cache("last_30"), load_rezdy_stats_cache("next_30"))
    derived = _derive_dashboard_metrics(merged)
    derived["today_focus"] = [
        {"label": "Flights cancelled", "value": f"{today_state['flights_cancelled']} / {today_state['flights_total']}" if today_state['flights_total'] else "0"},
        {"label": "Cancelled passengers", "value": str(today_state['passengers_cancelled'])},
        {"label": "Refund value", "value": f"${today_state['refunds']:,.0f}"},
        {"label": "Tomorrow confirmed pax", "value": str(derived.get('tomorrow_passengers', 0))},
    ]
    return derived


def dashboard_payload() -> Dict[str, Any]:
    snap = current_operation_snapshot()
    today_ops = next((r for r in reversed(get_historical_daily_stats()) if r.get("day") == "today"), {})
    tomorrow_ops = next((r for r in reversed(get_historical_daily_stats()) if r.get("day") == "tomorrow"), {})

    insights: List[Dict[str, str]] = []
    today_pickup_summary = _top_pickup_sentence(snap.get('today_pickup_breakdown', []), limit=3)
    last_pickup_summary = _top_pickup_sentence(snap.get('pickup_breakdown', []), limit=3)
    next_pickup_summary = _top_pickup_sentence(snap.get('future_pickup_breakdown', []), limit=3)

    if snap.get("today_whole_day_cancelled"):
        insights.append({
            "title": "Today is fully cancelled.",
            "detail": f"{snap.get('today_flights_cancelled', 0)} flights are cancelled today, affecting {snap.get('today_cancelled_passengers', 0)} passengers and ${snap.get('today_refunds', 0):,.0f} in refunds.",
        })
    else:
        insights.append({
            "title": f"{snap['today_orders']} confirmed orders are live today.",
            "detail": f"That equals {snap['today_passengers']} confirmed passengers, ${snap['today_revenue']:,.0f} locked in today, and pickup pressure is heaviest at {today_pickup_summary}.",
        })

    if snap["top_source"] != "Unknown":
        insights.append({
            "title": f"{snap['top_source']} is driving the last 30 days.",
            "detail": f"It remains the strongest source by confirmed revenue, with {snap['last_30']['confirmed_orders']} confirmed bookings tracked across the current 30-day window.",
        })
    if snap["top_flight"] != "Unknown":
        insights.append({
            "title": f"{snap['top_flight']} is the busiest slot in the last 30 days.",
            "detail": f"That slot carries {snap.get('top_flight_passengers', 0)} confirmed passengers in the current 30-day window.",
        })
    insights.append({
        "title": "Pickup hotspots are now tracked both backward and forward.",
        "detail": f"Last 30 days most common pickups: {last_pickup_summary}. Next 30 days most common pickups: {next_pickup_summary}.",
    })
    insights.append({
        "title": "Next 30 days separate confirmed forward demand from cancellations.",
        "detail": f"Opsly is tracking {snap['next_30']['confirmed_orders']} confirmed forward bookings, {snap['next_30']['cancelled_orders']} cancelled forward bookings, and ${snap['next_30']['confirmed_revenue']:,.0f} in confirmed forward revenue.",
    })
    insights.append({
        "title": f"{snap['last_30']['busiest_day']} was the busiest recent booking day.",
        "detail": f"{snap['last_30']['busiest_day_bookings']} confirmed bookings landed that day, while the strongest recent day revenue peaked at ${snap['last_30']['strongest_day_revenue']:,.0f}.",
    })

    hero_today = {
        "headline": "Today is fully cancelled" if snap.get("today_whole_day_cancelled") else f"{snap['today_orders']} confirmed orders live today",
        "subline": (
            f"{snap.get('today_cancelled_passengers', 0)} passengers are in the cancellation queue and {snap.get('today_flights_cancelled', 0)} flights are down."
            if snap.get("today_whole_day_cancelled")
            else f"{snap['today_passengers']} confirmed passengers are active today, with {snap['today_cancelled_orders']} cancellations tracked separately and top pickups at {today_pickup_summary}."
        ),
        "olivia_title": "Olivia says",
        "olivia_text": f"Today is shaped by {snap['today_orders']} confirmed orders, {snap['today_passengers']} confirmed passengers, ${snap['today_revenue']:,.0f} in live value, and pickup pressure at {today_pickup_summary}.",
    }

    chart_notes = {
        "last_30_days": _window_caption("Last 30 days", snap["last_30"]),
        "next_30_days": _window_caption("Next 30 days", snap["next_30"]),
    }

    return {
        "summary": snap,
        "today_ops": today_ops,
        "tomorrow_ops": tomorrow_ops,
        "insights": insights,
        "hero_today": hero_today,
        "chart_notes": chart_notes,
    }

def answer_question(question: str) -> Dict[str, Any]:
    q = (question or "").strip().lower()
    days = 30
    source = None
    day_match = re.search(r"(\d+)\s*day", q)
    if day_match:
        days = int(day_match.group(1))
    if "getyourguide" in q or "gyg" in q:
        source = "GetYourGuide"
    elif "viator" in q:
        source = "Viator"

    if "next" in q and "revenue" in q:
        value = future_revenue(days_ahead=days, source=source)
        src_txt = f" from {source}" if source else ""
        return {"answer": f"Projected booked revenue{src_txt} in the next {days} days is ${value:,.0f}.", "metric": "future_revenue", "value": value}

    if "how many" in q and ("booking" in q or "order" in q):
        value = bookings_count(days=days, source=source)
        src_txt = f" from {source}" if source else ""
        return {"answer": f"There are {value} bookings{src_txt} in the last {days} days based on the data currently cached in Opsly.", "metric": "bookings_count", "value": value}

    if "passenger" in q and ("today" in q or "tomorrow" in q):
        snap = current_operation_snapshot()
        if "tomorrow" in q:
            value = snap["tomorrow_passengers"]
            return {"answer": f"Tomorrow there are {value} passengers booked across the cached manifests.", "metric": "tomorrow_passengers", "value": value}
        value = snap["today_passengers"]
        return {"answer": f"Today there are {value} passengers booked across the cached manifests.", "metric": "today_passengers", "value": value}

    if "revenue" in q:
        value = revenue_count(days=days, source=source)
        src_txt = f" from {source}" if source else ""
        return {"answer": f"Revenue{src_txt} in the last {days} days is ${value:,.0f} from the current local analytics data.", "metric": "revenue", "value": value}

    if "cancel" in q or "refund" in q:
        stats = cancellations_count(days=days, source=source)
        src_txt = f" from {source}" if source else ""
        return {"answer": f"In the last {days} days{src_txt}, there were {stats['orders']} cancelled bookings affecting {stats['passengers']} passengers, worth ${stats['revenue']:,.0f}.", "metric": "cancellations", "value": stats}

    if "top source" in q or "best source" in q or "top agent" in q:
        rows = source_breakdown(days=days)
        if rows:
            top = rows[0]
            return {"answer": f"Top source in the last {days} days is {top['source']} with {top['bookings']} bookings and ${top['revenue']:,.0f} revenue.", "metric": "top_source", "value": top}
        return {"answer": "No source data is available yet.", "metric": "top_source", "value": None}

    snap = current_operation_snapshot()
    return {
        "answer": (
            "Olivia can answer bookings, revenue, next 30 day revenue, passengers today or tomorrow, "
            "cancellations, refunds, and top source questions. "
            f"Right now there are {snap['today_orders']} orders today and projected revenue of ${snap['future_revenue_30d']:,.0f} in the next 30 days."
        ),
        "metric": "help",
        "value": None,
    }
