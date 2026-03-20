from io import BytesIO
import ast
import json
import re
from pathlib import Path

from flask import Blueprint, render_template, request, send_file, abort, flash, redirect, url_for, jsonify, current_app
from flask_login import login_required
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

from app.services.rezdy import get_bookings_for_day, get_order_detail
from app.services.missive import send_email
from app.services.ops_state import rebuild_ops_state_from_manifests, apply_ops_state_to_manifests, load_ops_state

manifest = Blueprint("manifest", __name__, url_prefix="/manifest")

TICKET_PRICE = 299


def _snapshot_dir():
    path = Path(current_app.instance_path) / "day_snapshots"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _date_key(day_offset):
    from datetime import datetime, timedelta
    return (datetime.now().date() + timedelta(days=day_offset)).strftime("%Y-%m-%d")


def _snapshot_path(day_offset):
    label = "tomorrow" if day_offset == 1 else "today"
    return _snapshot_dir() / f"{label}_{_date_key(day_offset)}.json"


def _save_snapshot(day_offset, manifests):
    path = _snapshot_path(day_offset)
    payload = {"date": _date_key(day_offset), "day_offset": day_offset, "manifests": manifests}
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_snapshot(day_offset):
    path = _snapshot_path(day_offset)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload.get("manifests") or []
    except Exception:
        return None


def _pickup_obj(value):
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        txt = value.strip()
        for parser in (json.loads, ast.literal_eval):
            try:
                parsed = parser(txt)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
    return {}


def _walk(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk(item)


def _clean_contact(value):
    if value is None:
        return ""
    value = str(value).strip()
    if not value:
        return ""
    if value.lower() in {"no email", "no phone", "n/a", "na", "none", "null", "-", "no mobile"}:
        return ""
    return value


def _extract_emails(text):
    if not text:
        return []
    return list(dict.fromkeys(re.findall(r'[\w\.-]+@[\w\.-]+\.\w+', str(text))))


def _extract_phones(text):
    if not text:
        return []
    return list(dict.fromkeys(re.findall(r'(?:\+?\d[\d\s\-\(\)]{7,}\d)', str(text))))


def _clean_pickup_location(value):
    pickup = _pickup_obj(value)
    if pickup:
        return pickup.get("location_short") or pickup.get("location_full") or pickup.get("pickup_address") or ""
    if isinstance(value, str):
        return value
    return ""


def _first_real_email_from_anything(obj):
    for node in _walk(obj):
        if not isinstance(node, dict):
            continue
        for key in ("order_email", "email", "customer_email", "contact_email", "customerEmail", "contactEmail", "billingEmail", "leadEmail"):
            value = _clean_contact(node.get(key))
            if value and "@" in value:
                return value
    return ""


def _first_real_phone_from_anything(obj):
    for node in _walk(obj):
        if not isinstance(node, dict):
            continue
        for key in ("order_phone", "phone", "mobile", "customer_mobile", "contact_phone", "customerPhone", "customerMobile"):
            value = _clean_contact(node.get(key))
            if value:
                phones = _extract_phones(value)
                if phones:
                    return phones[0]
                return value
    return ""


def _normalise_passenger(passenger, fallback_email="", fallback_mobile="", pickup_location="", pickup_time=""):
    passenger = passenger or {}
    raw = passenger.get("raw") if isinstance(passenger.get("raw"), dict) else {}

    email = (
        _clean_contact(passenger.get("email"))
        or _clean_contact(passenger.get("order_email"))
        or _clean_contact(passenger.get("customer_email"))
        or _clean_contact(passenger.get("contact_email"))
        or _clean_contact(raw.get("email"))
        or _clean_contact(raw.get("order_email"))
        or _clean_contact(fallback_email)
    )

    mobile = (
        _clean_contact(passenger.get("mobile"))
        or _clean_contact(passenger.get("phone"))
        or _clean_contact(passenger.get("order_phone"))
        or _clean_contact(passenger.get("customer_mobile"))
        or _clean_contact(passenger.get("contact_phone"))
        or _clean_contact(raw.get("phone"))
        or _clean_contact(raw.get("mobile"))
        or _clean_contact(raw.get("order_phone"))
        or _clean_contact(fallback_mobile)
    )

    weight_lines = passenger.get("weight_lines") or ([f"{passenger.get('weight')} kg"] if passenger.get("weight") not in (None, "", 0) else [])

    return {
        "name": passenger.get("name") or passenger.get("full_name") or "Passenger",
        "weight": passenger.get("weight"),
        "weight_lines": weight_lines,
        "email": email or "No email",
        "mobile": mobile or "No phone",
        "pickup_location": pickup_location,
        "pickup_time": pickup_time,
    }


def _normalise_order(order):
    order = dict(order or {})
    pickup = _pickup_obj(order.get("pickup"))
    pickup_location = pickup.get("location_short") or order.get("pickup_location") or _clean_pickup_location(order.get("pickup")) or ""
    pickup_time = pickup.get("pickup_time") or order.get("pickup_time") or ""
    pickup_date = pickup.get("pickup_date") or order.get("pickup_date") or ""
    pickup_address = pickup.get("pickup_address") or order.get("pickup_address") or ""

    emails = []
    mobiles = []

    for key in ("email", "order_email", "customer_email", "contact_email"):
        value = _clean_contact(order.get(key))
        if value:
            emails.extend(_extract_emails(value))

    for key in ("mobile", "phone", "order_phone", "customer_mobile", "contact_phone"):
        value = _clean_contact(order.get(key))
        if value:
            mobiles.extend(_extract_phones(value))

    raw_passengers = order.get("passengers") or []
    passengers = []
    for p in raw_passengers:
        px = _normalise_passenger(p, pickup_location=pickup_location, pickup_time=pickup_time)
        passengers.append(px)
        if px.get("email") and px.get("email") != "No email":
            emails.extend(_extract_emails(px["email"]))
        if px.get("mobile") and px.get("mobile") != "No phone":
            mobiles.extend(_extract_phones(px["mobile"]))

    if not emails:
        extra_email = _first_real_email_from_anything(order)
        if extra_email:
            emails.append(extra_email)

    if not mobiles:
        extra_phone = _first_real_phone_from_anything(order)
        if extra_phone:
            mobiles.append(extra_phone)

    emails = list(dict.fromkeys([e for e in emails if _clean_contact(e)]))
    mobiles = list(dict.fromkeys([m for m in mobiles if _clean_contact(m)]))

    if raw_passengers:
        passengers = [
            _normalise_passenger(
                p,
                fallback_email=(emails[0] if emails else ""),
                fallback_mobile=(mobiles[0] if mobiles else ""),
                pickup_location=pickup_location,
                pickup_time=pickup_time,
            )
            for p in raw_passengers
        ]

    order["pickup"] = pickup
    order["pickup_location"] = pickup_location or "No pickup location"
    order["pickup_time"] = pickup_time or "No pickup time"
    order["pickup_date"] = pickup_date
    order["pickup_address"] = pickup_address
    order["email"] = emails[0] if emails else (_clean_contact(order.get("order_email")) or "No email")
    order["mobile"] = mobiles[0] if mobiles else (_clean_contact(order.get("order_phone")) or "No phone")
    order["passengers"] = passengers
    order["customer_name"] = order.get("customer_name") or order.get("order_name") or "Customer"
    return order


def _normalise_manifests(manifests):
    cleaned = []
    for manifest_item in manifests or []:
        m = dict(manifest_item or {})
        cleaned_pickups = []
        for pickup in m.get("pickup_summary", []) or []:
            cleaned_pickups.append({
                "date": pickup.get("date") or "",
                "time": pickup.get("time") or "",
                "location": _clean_pickup_location(pickup.get("location")) or pickup.get("location") or "",
            })
        m["pickup_summary"] = cleaned_pickups
        m["orders"] = [_normalise_order(order) for order in (m.get("orders") or [])]
        cleaned.append(m)
    return cleaned


def _load_or_build_snapshot(day_offset, force_refresh=False):
    cached = None if force_refresh else _load_snapshot(day_offset)
    if cached is not None:
        return _normalise_manifests(cached)
    manifests = _normalise_manifests(get_bookings_for_day(day_offset=day_offset))
    _save_snapshot(day_offset, manifests)
    return manifests


def _find_order_in_snapshots(order_number):
    for day_offset in (0, 1):
        manifests = _load_snapshot(day_offset) or []
        for manifest_item in manifests:
            for order in manifest_item.get("orders", []) or []:
                if (order.get("order_number") or "").strip() == order_number:
                    return _normalise_order(order)
    return None


def _apply_state(day, manifests):
    manifests = _normalise_manifests(manifests)
    rebuild_ops_state_from_manifests(day, manifests, ticket_price=TICKET_PRICE)
    manifests = apply_ops_state_to_manifests(day, manifests)
    return manifests, load_ops_state(day)


def _summary(manifests, state):
    flights = len(manifests or [])
    passengers = sum(int(order.get("pax_total") or 0) for manifest_item in manifests for order in (manifest_item.get("orders") or []))
    orders = sum(len(manifest_item.get("orders") or []) for manifest_item in manifests)
    cancelled_flights = int(state.get("stats", {}).get("flights_cancelled", 0))
    cancelled_passengers = int(state.get("stats", {}).get("passengers_cancelled", 0))
    revenue = passengers * TICKET_PRICE
    refunds = cancelled_passengers * TICKET_PRICE
    return {
        "flights": flights,
        "orders": orders,
        "passengers": passengers,
        "cancelled_flights": cancelled_flights,
        "cancelled_passengers": cancelled_passengers,
        "revenue": revenue,
        "refunds": refunds,
        "whole_day_cancelled": bool(state.get("whole_day_cancelled", False)),
    }


def _resolve_order(order_number, day):
    day_offset = 1 if day == "tomorrow" else 0
    order = None
    try:
        order = get_order_detail(order_number, day_offset=day_offset)
    except Exception:
        order = None
    if not order:
        order = _find_order_in_snapshots(order_number)
    if not order:
        return None
    return _normalise_order(order)


def _pickup_subject(order):
    order_number = order.get("order_number") or "Order"
    return f"Pickup Information - {order_number}"


def _pickup_body(order):
    customer = order.get("customer_name") or "Customer"
    pickup_location = order.get("pickup_location") or "-"
    pickup_time = order.get("pickup_time") or "-"
    pickup_date = order.get("pickup_date") or "-"
    flight_slot = order.get("flight_slot_full") or order.get("flight_slot") or order.get("slot_display_full") or "-"
    return f"""
    <div>Hello {customer},</div>
    <br>
    <div>Here is your pickup information for your upcoming flight with Whitsunday Air Tours.</div>
    <br>
    <div><strong>Order number:</strong> {order.get('order_number') or '-'}</div>
    <div><strong>Pickup date:</strong> {pickup_date}</div>
    <div><strong>Pickup time:</strong> {pickup_time}</div>
    <div><strong>Pickup location:</strong> {pickup_location}</div>
    <div><strong>Flight:</strong> {flight_slot}</div>
    <br>
    <div>Please be ready a few minutes early.</div>
    <br>
    <div>Kind Regards,</div>
    <div>Opsly / Whitsunday Air Tours</div>
    """


def _send_pickup_for_order(order, fallback_to_admin=False):
    email = _clean_contact(order.get("email"))
    if not email or "@" not in email:
        resolved = _first_real_email_from_anything(order)
        if resolved:
            email = resolved
    if not email or "@" not in email:
        if fallback_to_admin:
            email = "opsly.aip@gmail.com"
        else:
            return False, "No email found"
    send_email(email, _pickup_subject(order), _pickup_body(order))
    return True, email


@manifest.route("/")
@login_required
def home():
    error = None
    manifests = []
    day = request.args.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    day_offset = 1 if active_day == "tomorrow" else 0

    try:
        manifests = _load_or_build_snapshot(day_offset)
        manifests, state = _apply_state(active_day, manifests)
        stats = _summary(manifests, state)
    except Exception as e:
        error = str(e)
        state = load_ops_state(active_day)
        stats = _summary([], state)

    return render_template(
        "manifest.html",
        manifests=manifests,
        error=error,
        active_day=active_day,
        stats=stats,
        page_name="Flight Deck",
    )


@manifest.route("/refresh")
@login_required
def refresh_snapshot():
    day = request.args.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    day_offset = 1 if active_day == "tomorrow" else 0
    manifests = _load_or_build_snapshot(day_offset, force_refresh=True)
    rebuild_ops_state_from_manifests(active_day, manifests, ticket_price=TICKET_PRICE)
    flash(f"{active_day.title()} refreshed")
    return redirect(url_for("manifest.home", day=active_day))


@manifest.route("/send-pickup/<order_number>", methods=["POST"])
@login_required
def send_pickup(order_number):
    day = request.form.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    order = _resolve_order(order_number, active_day)
    if not order:
        flash(f"Order {order_number} not found")
        return redirect(url_for("manifest.home", day=active_day))
    try:
        ok, result = _send_pickup_for_order(order)
        if ok:
            flash(f"Pickup info sent to {result}")
        else:
            flash(f"Could not send pickup info for {order_number}: {result}")
    except Exception as e:
        flash(f"Pickup send failed for {order_number}: {e}")
    return redirect(url_for("manifest.home", day=active_day))


@manifest.route("/send-pickup-flight", methods=["POST"])
@login_required
def send_pickup_flight():
    day = request.form.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    slot = request.form.get("slot", "").strip()
    day_offset = 1 if active_day == "tomorrow" else 0

    sent = 0
    failed = 0

    manifests = _load_or_build_snapshot(day_offset)
    manifests, _state = _apply_state(active_day, manifests)

    for manifest_item in manifests:
        if (manifest_item.get("slot_display_full") or "").strip() != slot:
            continue
        for order in manifest_item.get("orders", []) or []:
            try:
                ok, _ = _send_pickup_for_order(order)
                if ok:
                    sent += 1
                else:
                    failed += 1
            except Exception:
                failed += 1
        break

    flash(f"Flight pickup info sent: {sent} sent, {failed} failed")
    return redirect(url_for("manifest.home", day=active_day))


@manifest.route("/send-pickup-day", methods=["POST"])
@login_required
def send_pickup_day():
    day = request.form.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    day_offset = 1 if active_day == "tomorrow" else 0

    sent = 0
    failed = 0

    manifests = _load_or_build_snapshot(day_offset)
    manifests, _state = _apply_state(active_day, manifests)

    for manifest_item in manifests:
        for order in manifest_item.get("orders", []) or []:
            try:
                ok, _ = _send_pickup_for_order(order)
                if ok:
                    sent += 1
                else:
                    failed += 1
            except Exception:
                failed += 1

    flash(f"Whole day pickup info sent: {sent} sent, {failed} failed")
    return redirect(url_for("manifest.home", day=active_day))


@manifest.route("/ops-summary")
@login_required
def ops_summary():
    today_manifests = _load_or_build_snapshot(0)
    tomorrow_manifests = _load_or_build_snapshot(1)

    today_manifests, today_state = _apply_state("today", today_manifests)
    tomorrow_manifests, tomorrow_state = _apply_state("tomorrow", tomorrow_manifests)

    return jsonify({
        "today": _summary(today_manifests, today_state),
        "tomorrow": _summary(tomorrow_manifests, tomorrow_state),
        "ticket_price": TICKET_PRICE,
    })


@manifest.route("/order/<order_number>")
@login_required
def order_detail(order_number):
    day = request.args.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    order = _resolve_order(order_number, active_day)
    if not order:
        abort(404)
    return render_template("order_detail.html", order=order, active_day=active_day)


@manifest.route("/order/<order_number>/json")
@login_required
def order_json(order_number):
    day = request.args.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    order = _resolve_order(order_number, active_day)
    if not order:
        abort(404)
    return render_template("order_json.html", order=order, active_day=active_day)


@manifest.route("/export")
@login_required
def export_pdf():
    day = request.args.get("day", "today").strip().lower()
    active_day = "tomorrow" if day == "tomorrow" else "today"
    slot_iso = request.args.get("slot", "").strip()
    day_offset = 1 if active_day == "tomorrow" else 0

    manifests = _load_or_build_snapshot(day_offset)
    manifests, _state = _apply_state(active_day, manifests)

    selected = None
    for m in manifests:
        if m.get("slot_iso") == slot_iso:
            selected = m
            break

    if not selected:
        abort(404)

    buffer = BytesIO()
    pdf = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 50

    pdf.setFont("Helvetica-Bold", 18)
    pdf.drawString(40, y, "Opsly Flight Deck Manifest")
    y -= 24

    pdf.setFont("Helvetica", 11)
    pdf.drawString(40, y, f"Flight: {selected.get('slot_display_full', '-')}")
    y -= 16
    pdf.drawString(40, y, f"Day: {active_day.title()}")
    y -= 16
    pdf.drawString(40, y, f"Total Pax: {selected.get('total_pax', 0)}")
    y -= 16
    total_weight = selected.get("total_weight")
    pdf.drawString(40, y, f"Total Weight: {total_weight} kg" if total_weight is not None else "Total Weight: Not available")
    y -= 24

    for order in selected.get("orders", []):
        if y < 100:
            pdf.showPage()
            y = height - 50
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(40, y, f"{order.get('customer_name', 'Customer')} - {order.get('order_number', '-')}")
        y -= 14
        pdf.setFont("Helvetica", 10)
        pdf.drawString(55, y, f"Email: {order.get('email', '-')}")
        y -= 12
        pdf.drawString(55, y, f"Mobile: {order.get('mobile', '-')}")
        y -= 12
        pdf.drawString(55, y, f"Pickup: {order.get('pickup_time', '-')} - {order.get('pickup_location', '-')}")
        y -= 18

    pdf.save()
    buffer.seek(0)
    return send_file(
        buffer,
        as_attachment=True,
        download_name=f"manifest_{active_day}_{selected.get('slot','flight')}.pdf",
        mimetype="application/pdf",
    )
