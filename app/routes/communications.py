import re
from flask import Blueprint, render_template
from flask_login import login_required
from app.services import missive

communications = Blueprint("communications", __name__, url_prefix="/communications")

def _clean_sender_name(name):
    if hasattr(missive, "clean_sender_name"):
        return missive.clean_sender_name(name)
    return name or "Unknown"

def _format_timestamp(value):
    if hasattr(missive, "format_timestamp"):
        return missive.format_timestamp(value)
    return value or ""

def _get_conversations():
    conversations = []
    for fn_name in ("get_recent_conversations", "get_inbox_conversations", "get_conversations", "get_wat_inbox_conversations", "get_order_emails"):
        fn = getattr(missive, fn_name, None)
        if fn:
            try:
                conversations = fn(limit=100) or []
                if conversations:
                    break
            except Exception:
                pass
    return conversations

@communications.route("/")
@communications.route("")
@login_required
def inbox():
    conversations = _get_conversations()
    cleaned = []

    for convo in conversations:
        subject = convo.get("subject") or convo.get("latest_message_subject") or "(No subject)"
        lower = subject.lower()
        if any(x in lower for x in ("new confirmed order", "product sold", "order cancelled", "cancelled order")):
            continue

        authors = convo.get("authors") or []
        sender = _clean_sender_name(authors[0].get("name")) if authors else "Unknown"

        cleaned.append({
            "id": convo.get("id", ""),
            "subject": subject,
            "sender": sender,
            "timestamp": _format_timestamp(convo.get("last_activity_at")),
        })

    return render_template("communications.html", conversations=cleaned)
