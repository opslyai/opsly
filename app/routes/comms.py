from flask import Blueprint, render_template
from flask_login import login_required
from app.services.missive import (
    get_passenger_conversations,
    get_conversation_by_id,
    get_conversation_messages,
    clean_sender_name,
    parse_subject,
    format_timestamp,
)

comms = Blueprint("comms", __name__, url_prefix="/communications")

@comms.route("/")
@comms.route("")
@login_required
def inbox():
    conversations = get_passenger_conversations(limit=50)
    parsed_conversations = []

    for convo in conversations:
        subject = convo.get("subject") or convo.get("latest_message_subject") or "(No subject)"
        authors = convo.get("authors") or []
        sender = clean_sender_name(authors[0].get("name")) if authors else "Unknown"
        parsed = parse_subject(subject)

        parsed_conversations.append({
            "id": convo.get("id", ""),
            "subject": subject,
            "sender": sender,
            "summary_type": parsed["summary_type"],
            "order_number": parsed["order_number"],
            "customer_name": parsed["customer_name"],
            "agent": parsed["agent"],
            "status": parsed["status"],
            "preview": convo.get("latest_message_subject") or "No preview available",
            "timestamp": format_timestamp(convo.get("last_activity_at")),
        })

    return render_template("comms.html", conversations=parsed_conversations)

@comms.route("/<conversation_id>")
@login_required
def conversation_detail(conversation_id):
    payload = get_conversation_by_id(conversation_id)

    conversation = None
    if isinstance(payload, dict) and "conversations" in payload:
        conversations = payload.get("conversations") or []
        conversation = conversations[0] if conversations else None
    elif isinstance(payload, dict):
        conversation = payload

    if not conversation:
        conversation = {"id": conversation_id}

    subject = conversation.get("subject") or conversation.get("latest_message_subject") or "(No subject)"
    parsed = parse_subject(subject)

    authors = conversation.get("authors") or []
    sender = clean_sender_name(authors[0].get("name")) if authors else "Unknown"

    messages = get_conversation_messages(conversation_id, limit=10)
    latest_message = messages[0] if messages else {}

    body = (
        latest_message.get("body")
        or latest_message.get("text")
        or latest_message.get("preview")
        or "No message body available."
    )

    detail = {
        "id": conversation.get("id", ""),
        "subject": subject,
        "sender": sender,
        "summary_type": parsed["summary_type"] or subject,
        "order_number": parsed["order_number"],
        "customer_name": parsed["customer_name"],
        "agent": parsed["agent"],
        "status": parsed["status"],
        "messages_count": conversation.get("messages_count", 0),
        "attachments_count": conversation.get("attachments_count", 0),
        "web_url": conversation.get("web_url", ""),
        "body": body,
        "timestamp": format_timestamp(conversation.get("last_activity_at")),
    }

    return render_template("conversation_detail.html", detail=detail)
