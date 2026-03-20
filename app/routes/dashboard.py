from flask import Blueprint, jsonify, render_template
from flask_login import current_user, login_required

from app.services.analytics import clear_rezdy_stats_cache, dashboard_payload, ensure_daily_rezdy_cache, save_rezdy_stats_cache
from app.services.rezdy import get_recent_booking_stats, get_upcoming_booking_stats
from app.services.weather import get_airlie_weather


dashboard = Blueprint("dashboard", __name__)


@dashboard.route("/")
def landing():
    return render_template("landing.html")


@dashboard.route("/dashboard")
@login_required
def dashboard_home():
    ensure_daily_rezdy_cache()
    payload = dashboard_payload()
    weather = get_airlie_weather()
    return render_template(
        "dashboard.html",
        email=current_user.email,
        dashboard_data=payload,
        weather=weather,
    )


@dashboard.route("/dashboard/pull-rezdy-stats", methods=["POST"])
@login_required
def pull_rezdy_stats():
    payload = get_recent_booking_stats(days=30)
    next_payload = get_upcoming_booking_stats(days=30)
    save_rezdy_stats_cache(payload, window="last_30")
    save_rezdy_stats_cache(next_payload, window="next_30")
    return jsonify({
        "ok": True,
        "message": "Rezdy last 30 and next 30 day stats refreshed.",
        "generated_at": payload.get("generated_at"),
        "generated_at_next": next_payload.get("generated_at"),
    })


@dashboard.route("/dashboard/clear-rezdy-stats", methods=["POST"])
@login_required
def clear_rezdy_stats():
    clear_rezdy_stats_cache()
    return jsonify({
        "ok": True,
        "message": "Rezdy cached stats cleared.",
    })


@dashboard.route("/about")
def about():
    return render_template("about.html")


@dashboard.route("/contact")
def contact():
    return render_template("contact.html", contact_email="opsly.aip@gmail.com")




@dashboard.route("/olivia-ai")
def olivia_marketing():
    return render_template("olivia_marketing.html")

@dashboard.route("/pricing")
def pricing():
    return render_template("pricing.html")
