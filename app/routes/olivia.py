from flask import Blueprint, render_template, request, jsonify
from flask_login import login_required

from app.services.analytics import dashboard_payload
from app.services.olivia_agent import ask_olivia, starter_olivia_payload
from app.services.weather import get_airlie_weather

olivia_bp = Blueprint('olivia', __name__)


@olivia_bp.route('/olivia')
@login_required
def olivia_page():
    payload = dashboard_payload()
    weather = get_airlie_weather()
    stats = payload.get("summary", {})
    quick_questions = [
        "How many GetYourGuide bookings in the last 30 days?",
        "How many bookings in the last 5 days?",
        "Revenue in the next 30 days?",
        "Revenue in the last 30 days?",
        "How many passengers are flying tomorrow?",
        "Which source has the most bookings this month?",
        "How many cancellations in the last 30 days?",
        "What are today's main ops issues?",
    ]
    starter = starter_olivia_payload()
    return render_template('olivia.html', stats=stats, weather=weather, quick_questions=quick_questions, insights=payload.get('insights', []), source_breakdown=stats.get('source_breakdown', []), starter=starter)


@olivia_bp.route('/api/olivia/query', methods=['POST'])
@login_required
def olivia_query():
    data = request.get_json(silent=True) or {}
    q = data.get('query', '')
    result = ask_olivia(q)
    return jsonify({'ok': True, 'query': q, **result})
