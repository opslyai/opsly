from threading import Thread, Lock
from datetime import datetime
import json
import os
import time
import uuid

from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, current_app
from flask_login import login_required

from app.services.rezdy import get_bookings_for_day, get_order_detail
from app.services.missive import send_email
from app.services.ops_state import rebuild_ops_state_from_manifests, apply_ops_state_to_manifests, mark_whole_day_cancelled, mark_flight_cancelled, mark_order_cancelled, load_ops_state, dashboard_stats

cancellations = Blueprint("cancellations", __name__, url_prefix="/cancellations")

STATE_FILE = os.path.expanduser("~/opsly/instance/cancellation_state.json")
STATE_LOCK = Lock()

JOBS = {}
JOBS_LOCK = Lock()


def _set_job(job_id, **updates):
    with JOBS_LOCK:
        job = JOBS.setdefault(job_id, {
            "status": "queued",
            "steps": [],
            "done": 0,
            "total": 0,
            "redirect_url": "/manifest"
        })
        step = updates.pop("append_step", None)
        if step:
            job.setdefault("steps", []).append(step)
        job.update(updates)
        return dict(job)

def _ensure_state_file():
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    if not os.path.exists(STATE_FILE):
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({}, f)


def _load_state():
    _ensure_state_file()
    with STATE_LOCK:
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}


def _save_state(state):
    _ensure_state_file()
    with STATE_LOCK:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)


def _day_key(day):
    return "tomorrow" if str(day).strip().lower() == "tomorrow" else "today"


def _mark_order_cancelled(day, order_number, email="", reason="other", status="sent", error=""):
    state = _load_state()
    day = _day_key(day)

    if day not in state:
        state[day] = {}

    state[day][order_number] = {
        "status": status,
        "email": email,
        "reason": reason,
        "error": error,
        "updated_at": datetime.utcnow().isoformat()
    }

    _save_state(state)


def _get_order_state(day, order_number):
    state = _load_state()
    day = _day_key(day)
    return state.get(day, {}).get(order_number, {})


def _extract_email(detail):
    condidates = []

    cdn = detail.get("contact") or {}
    if cdn.get("email"):
        condidates.append(cdn.get("email"))

    for guest in detail.get("guests", []) or []:
        if guest.get("email"):
            condidates.append(guest.get("email"))

    for field in detail.get("fields", []) or []:
        if field.get("label", "").lower() in {"email", "contact email", "contact_email"}:
            value = field.get("value")
            if value:
                condidates.append(value)

    for c  in condidates:
        if isinstance(c, str) and "@" in c:
            return c

    return ""



def _extract_name(detail):
    contact = detail.get("contact") or {}
    name = " ".join(part for part in [contact.get("firstName"), contact.get("lastName")] if part).strip()
    if name:
        return name
    guests = detail.get("guests", []) or []
    if guests:
        g = guests[0] or {}
        name = " ".join(part for part in [g.get("firstName"), g.get("lastName")] if part).strip()
        if name:
            return name
    return detail.get("orderNumber", "Customer")



def _send_cancel_email(order_number, reason, day):
    day_offset = 1 if str(day).strip().lower() == "tomorrow" else 0
    detail = get_order_detail(order_number, day_offset=day_offset)

    email = _extract_email(detail)
    name = _extract_name(detail)

    if not email:
        _mark_order_cancelled(day, order_number, email="", reason=reason, status="failed", error="No email found")
        return False, "No email found"
    
    if reason == "weather":
        subject = "Flight Cancelled due to Poor Weather"
    else:
        subject = "Flight Cancelled"

    body = f"""Hello {name},


Unfortunately your scenic flight has been cancelled.

If you would like to reschedule, please reply to this email and let us know what dates work for you.

If you would prefer a full refund, please reply to this email and we will process this for you.

We are very sorry for the inconvenience.

Kind regards,
Whitsunday Air Tours"""

    try:
        send_email(email, subject, body)
        _mark_order_cancelled(day, order_number, email=email, reason=reason, status="sent", error="")
        mark_order_cancelled(day, order_number)
        return True, email
    except Exception as e:
        _mark_order_cancelled(day, order_number, email=email, reason=reason, status="failed", error=str(e))
        return False, str(e)


def _attach_emails(manifests, day):
    for manifest in manifests or []:
        manifest["is_cancelled"] = False
        for order in manifest.get("orders", []) or []:
            order_number = order.get("order_number", "")
            order_state = _get_order_state(day, order_number)
            if order_state.get("email"):
                order["customer_email"] = order_state.get("email")
            order["cancellation_state"] = order_state
            order["is_cancelled"] = order_state.get("status") == "sent"
            if order["is_cancelled"]:
                manifest["is_cancelled"] = True
    return manifests



def _build_stats(manifests):
    flights_total = len(manifests or [])
    passengers_total = sum(int(m.get("pax") or 0) for m in (manifests or []))
    flights_cancelled = 0
    passengers_cancelled = 0

    for manifest in manifests or []:
        if manifest.get("is_cancelled"):
            flights_cancelled += 1
        for order in manifest.get("orders", []) or []:
            if order.get("is_cancelled"):
                passengers_cancelled += int(order.get("pax_total") or 0)

    return {
        "flights_total": flights_total,
        "passengers_total": passengers_total,
        "flights_cancelled": flights_cancelled,
        "passengers_cancelled": passengers_cancelled
    }


def _send_bulk_cancellations(app, order_numbers, reason, day, job_id=None):
    with app.app_context():
        total = len(order_numbers)
        if job_id:
            _set_job(job_id, status="running", total=total, done=0, append_step="Connecting to Missive...")
        if total == 0:
            if job_id:
                _set_job(job_id, status="done", append_step="No bookings found to cancel.")
            return

        for idx, order_number in enumerate(order_numbers, start=1):
            try:
                if job_id:
                    _set_job(job_id, done=idx-1, append_step=f"Sending email for {order_number} ({idx}/{total})...")
                sent, result = _send_cancel_email(order_number, reason, day)
                if job_id:
                    _set_job(job_id, done=idx, append_step=(f"Confirmed email sent to {result}" if sent else f"Failed for {order_number}: {result}"))
                time.sleep(0.15)
            except Exception as e:
                _mark_order_cancelled(day, order_number, email="", reason=reason, status="failed", error=str(e))
                if job_id:
                    _set_job(job_id, done=idx, append_step=f"Failed for {order_number}: {e}")
                print(f"[CANCELLATION ERROR] {order_number}: {e}")

        if job_id:
            _set_job(job_id, status="done", append_step="Cancellation run complete.")


@cancellations.route("/")
@login_required
def cancellations_home():
    day = request.args.get("day", "today")
    active_day = "tomorrow" if str(day).strip().lower() == "tomorrow" else "today"
    day_offset = 1 if active_day == "tomorrow" else 0
    manifests = []
    stats = {
        "flights_total": 0,
        "passengers_total": 0,
        "flights_cancelled": 0,
        "passengers_cancelled": 0
    }
    error = None

    try:
        manifests = get_bookings_for_day(day_offset=day_offset)
        manifests = _attach_emails(manifests, active_day)
        stats = _build_stats(manifests)
    except Exception as e:
        error = str(e)
    whole_day_cancelled = False
    if whole_day_cancelled:
        for manifest in manifests or []:
            manifest["is_cancelled"] = True
            for order in manifest.get("orders", []) or []:
                order["is_cancelled"] = True
        if isinstance(stats, dict):
            stats["flights_cancelled"] = stats.get("flights_total", stats.get("flights_cancelled", 0)) or len(manifests or [])
            stats["passengers_cancelled"] = stats.get("passengers_total", stats.get("passengers_cancelled", 0)) or sum(int(o.get("pax_total") or 0) for m in (manifests or []) for o in (m.get("orders", []) or []))

    return render_template(
        "cancellations.html",
        manifests=manifests,
        stats=stats,
        error=error,
        active_day=active_day, whole_day_cancelled=whole_day_cancelled)


@cancellations.route("/job/<job_id>")
@login_required
def cancellation_job(job_id):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"ok": False, "error": "Job not found"}), 404
    return jsonify({"ok": True, **job})


@cancellations.route("/cancel_preview")
@login_required
def cancel_preview():
    order_number = request.args.get("order_number")
    day = request.args.get("day", "today").lower().strip()
    day_offset = 1 if day == "tomorrow" else 0

    detail = get_order_detail(order_number, day_offset=day_offset)

    email = _extract_email(detail)
    name = _extract_name(detail)

    return jsonify({
        "customer_name": name,
        "customer_email": email,
        "order_number": order_number,
        "day": day
    })


@cancellations.route("/cancel-order/<order_number>", methods=["POST"])
@login_required
def cancel_order(order_number):
    reason = request.form.get("reason", "other")
    day = request.form.get("day", "today")
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.form.get("ajax") == "1"

    if wants_json:
        id=uuid.uuid4().hex
        _set_job(id, status="queued", steps=[f"Queued cancellation for {order_number}"], redirect_url=url_for("manifest.home", day=day), total=1, done=0)
        app = current_app._get_current_object()
        Thread(target=_send_bulk_cancellations, args=(app, [order_number], reason, day, id), daemon=True).start()
        return jsonify({"ok": True, "job_id": id, "redirect_url": url_for("manifest.home", day=day)})

    sent, result = _send_cancel_email(order_number, reason, day)

    if sent:
        mark_order_cancelled(day, order_number)
        flash(f"Cancellation email sent to {result}")
    else:
        flash(f"Could not send email: {result}")

    return redirect(url_for("manifest.home", day=day))


@cancellations.route("/cancel-flight", methods=["POST"])
@login_required
def cancel_flight():
    day = request.form.get("day", "today")
    slot = request.form.get("slot")
    reason = request.form.get("reason", "other")
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.form.get("ajax") == "1"

    day_offset = 1 if str(day).strip().lower() == "tomorrow" else 0

    try:
        manifests = get_bookings_for_day(day_offset=day_offset)
    except Exception as e:
        if wants_json:
            return jsonify({"ok": False, "error": str(e)}), 500
        flash(f"Could not load bookings for {day}: {e}")
        return redirect(url_for("manifest.home", day=day))

    order_numbers = []

    for manifest in manifests:
        if manifest.get("slot_display_full") == slot:
            for order in manifest.get("orders", []):
                order_number = order.get("order_number")
                if order_number:
                    order_numbers.append(order_number)
            break

    app = current_app._get_current_object()

    if wants_json:
        id=uuid.uuid4().hex
        _set_job(id, status="queued", steps=[f"Queued whole flight cancellation for {slot}"], redirect_url=url_for("manifest.home", day=day), total=len(order_numbers), done=0)
        Thread(target=_send_bulk_cancellations, args=(app, order_numbers, reason, day, id), daemon=True).start()
        return jsonify({"ok": True, "job_id": id, "redirect_url": url_for("manifest.home", day=day)})

    Thread(
        target=_send_bulk_cancellations,
        args=(app, order_numbers, reason, day),
        daemon=True
    ).start()

    flash(f"Sending cancellation emails for flight {slot} to {len(order_numberr)} orders")
    return redirect(url_for("manifest.home", day=day))


@cancellations.route("/cancel-day", methods=["POST"])
@login_required
def cancel_day():
    day = request.form.get("day", "today")
    reason = request.form.get("reason", "other")
    wants_json = request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.form.get("ajax") == "1"

    day_offset = 1 if str(day).strip().lower() == "tomorrow" else 0

    try:
        manifests = get_bookings_for_day(day_offset=day_offset)
    except Exception as e:
        if wants_json:
            return jsonify({"ok": False, "error": str(e)}), 500
        flash(f"Could not load bookings for {day}: {e}")
        return redirect(url_for("manifest.home", day=day))

    order_numbers = []
    for manifest in manifests:
        for order in manifest.get("orders", []):
            order_number = order.get("order_number")
            if order_number:
                order_numbers.append(order_number)

    app = current_app._get_current_object()

    if wants_json:
        id=uuid.uuid4().hex
        _set_job(id, status="queued", steps=[f"Queued whole day cancellation for {day}"], redirect_url=url_for("manifest.home", day=day), total=len(order_numbers), done=0)
        Thread(target=_send_bulk_cancellations, args=(app, order_numbers, reason, day, id), daemon=True).start()
        return jsonify({"ok": True, "job_id": id, "redirect_url": url_for("manifest.home", day=day)})

    Thread(
        target=_send_bulk_cancellations,
        args=(app, order_numbers, reason, day),
        daemon=True
    ).start()

    flash(f"Sending cancellation emails for entire {day} schedule to {len(order_numbers)} orders")
    return redirect(url_for("manifest.home", day=day))
