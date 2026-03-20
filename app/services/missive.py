import os
import re
from datetime import datetime

import requests

MISSIVE_API_BASE = os.getenv("MISSIVE_API_BASE", "https://public.missiveapp.com/v1").rstrip("/")
MISSIVE_PAT = os.getenv("MISSIVE_PAT")
MISSIVE_FROM_ADDRESS = os.getenv("MISSIVE_FROM_ADDRESS")
MISSIVE_WAT_RESOURCE = os.getenv("MISSIVE_WAT_RESOURCE")

ORDER_KEYWORDS = ("new confirmed order", "product sold")
AGENT_PATTERNS = [
    ("GetYourGuide", r"getyourguide|gyg"),
    ("Hero", r"\\bhero\\b"),
    ("Viator", r"viator"),
    ("Tripadvisor", r"tripadvisor"),
    ("Google Things To Do", r"things to do|google"),
    ("Happy Travels", r"happy travels"),
    ("Summer Travels", r"summer travels"),
    ("Sailing Whitsundays", r"sailing whitsundays"),
    ("Mr Travel", r"mr\\s*travel"),
    ("GSL", r"\\bgsl\\b"),
]

def _headers():
    if not MISSIVE_PAT:
        raise RuntimeError("MISSIVE_PAT is missing from .env")
    return {
        "Authorization": f"Bearer {MISSIVE_PAT}",
        "Content-Type": "application/json",
    }

def _request(method, path, *, params=None, json=None, timeout=30):
    url = f"{MISSIVE_API_BASE}{path}"
    response = requests.request(method, url, headers=_headers(), params=params, json=json, timeout=timeout)
    try:
        data = response.json()
    except Exception:
        data = {"raw": response.text}
    if response.status_code >= 400:
        raise RuntimeError(f"Missive API failed: {response.status_code} - {data}")
    return data

def _conversation_filters(resource_id=None):
    rid = resource_id or MISSIVE_WAT_RESOURCE
    filters = []
    if rid:
        filters.extend([
            {"team_inbox": rid},
            {"team_all": rid},
            {"shared_label": rid},
        ])
    filters.extend([
        {"inbox": "true"},
        {"all": "true"},
    ])
    return filters

def _list_conversations(limit=25, resource_id=None):
    limit = min(max(int(limit or 25), 1), 50)
    last_error = None
    for extra in _conversation_filters(resource_id=resource_id):
        try:
            data = _request("GET", "/conversations", params={"limit": limit, **extra})
            conversations = data.get("conversations") or []
            if conversations:
                return conversations
        except Exception as exc:
            last_error = exc
    if last_error:
        raise last_error
    return []

def clean_sender_name(name):
    if not name:
        return "Unknown"
    name = re.sub(r"\\s+via\\s+.*$", "", str(name), flags=re.I).strip()
    return name or "Unknown"

def format_timestamp(value):
    if value in (None, ""):
        return ""
    try:
        return datetime.fromtimestamp(int(value)).strftime("%d %b %Y %I:%M %p")
    except Exception:
        return str(value)

def parse_subject(subject):
    text = (subject or "").strip()
    lower = text.lower()
    order_match = re.search(r"\\b(WAT[A-Z0-9]{5,})\\b", text, flags=re.I)
    order_number = order_match.group(1).upper() if order_match else ""

    summary_type = "Passenger message"
    status = ""
    if "order cancelled" in lower or "cancelled order" in lower:
        summary_type = "Cancelled order"
        status = "cancelled"
    elif "new confirmed order" in lower or "product sold" in lower:
        summary_type = "New order"
        status = "confirmed"

    agent = ""
    for label, pattern in AGENT_PATTERNS:
        if re.search(pattern, lower, flags=re.I):
            agent = label
            break

    customer_name = ""
    for pattern in [
        r"new\s+confirmed\s+order\s+WAT[A-Z0-9]+\s+for\s+(.+?)(?:\s+from\s+.+)?$",
        r"product\s+sold\s+WAT[A-Z0-9]+\s+for\s+(.+?)(?:\s+from\s+.+)?$",
        r"(?:pax:\s*\d+\s*,\s*)([^|\-–—]+)",
        r"(?:for|guest|customer)\s*[:\-]?\s*([^|\-–—]+)",
        r"new confirmed order\s*[-:]*\s*([^|]+)",
        r"product sold\s*[-:]*\s*([^|]+)",
    ]:
        m = re.search(pattern, text, flags=re.I)
        if m:
            candidate = re.sub(r"\\bWAT[A-Z0-9]{5,}\\b", "", m.group(1), flags=re.I).strip(" -–—|")
            if candidate:
                customer_name = candidate
                break

    return {
        "summary_type": summary_type,
        "order_number": order_number,
        "customer_name": customer_name,
        "agent": agent,
        "status": status,
    }

def _is_order_subject(subject):
    lower = (subject or "").lower()
    if any(x in lower for x in ("cancelled", "refund", "void", "declined", "failed payment")):
        return False
    return any(keyword in lower for keyword in ORDER_KEYWORDS) and bool(re.search(r"\bWAT[A-Z0-9]+\b", subject or "", flags=re.I))

def _is_passenger_subject(subject):
    return not _is_order_subject(subject)

def send_email(to, subject, body):
    if not MISSIVE_FROM_ADDRESS:
        raise RuntimeError("MISSIVE_FROM_ADDRESS is missing from .env")
    if not to:
        raise RuntimeError("Recipient email is missing")
    payload = {
        "drafts": {
            "send": True,
            "subject": subject,
            "body": body,
            "from_field": {"address": MISSIVE_FROM_ADDRESS},
            "to_fields": [{"address": to}],
        }
    }
    return _request("POST", "/drafts", json=payload)

def create_draft(to, subject, body):
    return send_email(to, subject, body)

def get_conversations(limit=25):
    return _list_conversations(limit=limit)

def get_recent_conversations(limit=25):
    return get_conversations(limit=limit)

def get_inbox_conversations(limit=25):
    return get_conversations(limit=limit)

def get_wat_inbox_conversations(limit=25):
    return _list_conversations(limit=limit, resource_id=MISSIVE_WAT_RESOURCE)

def get_team_inbox_conversations(limit=25):
    return get_wat_inbox_conversations(limit=limit)

def get_passenger_conversations(limit=25):
    return [c for c in get_wat_inbox_conversations(limit=limit) if _is_passenger_subject(c.get("subject") or c.get("latest_message_subject") or "")]

def get_order_emails(limit=25):
    return [c for c in get_wat_inbox_conversations(limit=limit) if _is_order_subject(c.get("subject") or c.get("latest_message_subject") or "")]

def get_orders(limit=25):
    return get_order_emails(limit=limit)

def get_conversation(conversation_id):
    return _request("GET", f"/conversations/{conversation_id}")

def get_conversation_by_id(conversation_id):
    return get_conversation(conversation_id)

def get_messages_for_conversation(conversation_id, limit=10):
    data = _request("GET", f"/conversations/{conversation_id}/messages", params={"limit": min(max(int(limit or 10), 1), 10)})
    return data.get("messages") or []

def get_conversation_messages(conversation_id, limit=10):
    return get_messages_for_conversation(conversation_id, limit=limit)

def get_conversation_detail(conversation_id):
    convo = get_conversation_by_id(conversation_id)
    if isinstance(convo, dict):
        convo["messages"] = get_conversation_messages(conversation_id, limit=10)
    return convo

def list_conversations(limit=25):
    return get_wat_inbox_conversations(limit=limit)

def search_conversations(*args, **kwargs):
    return get_wat_inbox_conversations(limit=kwargs.get("limit", 25))

def get_today_orders(limit=25):
    return get_order_emails(limit=limit)

def get_new_orders_today(limit=25):
    return [c for c in get_order_emails(limit=limit) if "cancel" not in (c.get("subject") or c.get("latest_message_subject") or "").lower()]

def get_cancelled_orders_today(limit=25):
    return [c for c in get_order_emails(limit=limit) if "cancel" in (c.get("subject") or c.get("latest_message_subject") or "").lower()]

def get_order_by_number(order_number, limit=50):
    if not order_number:
        return None
    order_number = str(order_number).upper().strip()
    for convo in get_order_emails(limit=limit):
        subject = convo.get("subject") or convo.get("latest_message_subject") or ""
        if order_number in subject.upper():
            return convo
    return None

def fetch_conversations(limit=25):
    return get_wat_inbox_conversations(limit=limit)

def fetch_orders(limit=25):
    return get_order_emails(limit=limit)

def fetch_messages(conversation_id, limit=10):
    return get_conversation_messages(conversation_id, limit=limit)
