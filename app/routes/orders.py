import json
import re
from pathlib import Path

import requests
from flask import Blueprint, current_app, jsonify, render_template
from flask_login import login_required

from app.services import missive
from app.services.rezdy import get_order_detail as get_rezdy_order_detail, get_booking_by_order_number

orders = Blueprint("orders", __name__, url_prefix="/orders")

WAT_RE = re.compile(r"\bWAT[A-Z0-9]+\b", re.IGNORECASE)
EMAIL_RE = re.compile(r"[\w\.-]+@[\w\.-]+\.\w+")
PHONE_RE = re.compile(r"(?:\+?\d[\d\s\-\(\)]{7,}\d)")
ORDER_SUBJECT_ALLOW_RE = re.compile(r"(?:new\s+confirmed\s+order|product\s+sold)", re.IGNORECASE)
ORDER_SUBJECT_BLOCK_RE = re.compile(r"(?:cancelled?|refund|void|declined|failed\s+payment)", re.IGNORECASE)
NAME_LINE_RE = re.compile(r"(?:for|customer|guest|lead traveler)\s*[:\-]?\s*([A-Z][^\n\r|]+)", re.IGNORECASE)
TRAVELER_BLOCK_RE = re.compile(
    r"Traveler\s*(\d+)\s*:\s*(.*?)(?=(?:\nTraveler\s*\d+\s*:)|\Z)",
    re.IGNORECASE | re.DOTALL,
)


def _clean_sender_name(name):
    if hasattr(missive, "clean_sender_name"):
        try:
            return missive.clean_sender_name(name)
        except Exception:
            pass
    return name or "Unknown"


def _parse_subject(subject):
    if hasattr(missive, "parse_subject"):
        try:
            return missive.parse_subject(subject)
        except Exception:
            pass
    return {
        "summary_type": subject or "",
        "order_number": "",
        "customer_name": "",
        "agent": "",
        "status": "",
    }


def _format_timestamp(value):
    if hasattr(missive, "format_timestamp"):
        try:
            return missive.format_timestamp(value)
        except Exception:
            pass
    return value or ""


def _extract_order_number_from_text(text):
    if not text:
        return ""
    m = WAT_RE.search(str(text).upper())
    return m.group(0).upper() if m else ""


def _extract_emails(text):
    if not text:
        return []
    return list(dict.fromkeys(EMAIL_RE.findall(str(text))))


def _extract_phones(text):
    if not text:
        return []
    found = []
    for x in PHONE_RE.findall(str(text)):
        cleaned = re.sub(r"\s+", " ", x).strip()
        if cleaned not in found:
            found.append(cleaned)
    return found


def _first_value(*vals):
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s:
            return s
    return ""


def _conversation_texts(convo, messages):
    texts = []
    for item in [convo] + list(messages or []):
        if not isinstance(item, dict):
            continue
        for key in ("subject", "latest_message_subject", "snippet", "preview", "body", "text", "content"):
            value = item.get(key)
            if value:
                texts.append(str(value))
    return texts


def _is_actionable_order_subject(subject):
    subject = (subject or "").strip()
    if not subject:
        return False
    if ORDER_SUBJECT_BLOCK_RE.search(subject):
        return False
    return bool(ORDER_SUBJECT_ALLOW_RE.search(subject) and WAT_RE.search(subject))


def _get_conversations():
    conversations = []
    if hasattr(missive, "get_order_emails"):
        try:
            conversations = missive.get_order_emails(limit=150) or []
        except Exception:
            conversations = []
    if not conversations:
        for fn_name in ("get_recent_conversations", "get_inbox_conversations", "get_conversations", "get_wat_inbox_conversations"):
            fn = getattr(missive, fn_name, None)
            if fn:
                try:
                    conversations = fn(limit=150) or []
                    if conversations:
                        break
                except Exception:
                    pass
    return conversations


def _get_conversation(conversation_id):
    payload = missive.get_conversation_by_id(conversation_id) if hasattr(missive, "get_conversation_by_id") else {}
    if isinstance(payload, dict) and "conversations" in payload:
        conversations = payload.get("conversations") or []
        return conversations[0] if conversations else {}
    return payload if isinstance(payload, dict) else {}


def _get_messages(conversation_id):
    if hasattr(missive, "get_conversation_messages"):
        try:
            return missive.get_conversation_messages(conversation_id, limit=20) or []
        except Exception:
            pass
    return []


def _extract_order_number(convo, parsed, messages):
    candidates = [
        parsed.get("order_number") if isinstance(parsed, dict) else "",
        convo.get("subject", ""),
        convo.get("latest_message_subject", ""),
        convo.get("snippet", ""),
        convo.get("preview", ""),
        convo.get("body", ""),
        convo.get("text", ""),
        str(convo),
    ]
    for msg in messages or []:
        candidates.extend([
            msg.get("subject", ""),
            msg.get("preview", ""),
            msg.get("body", ""),
            msg.get("text", ""),
            str(msg),
        ])
    for text in candidates:
        order_number = _extract_order_number_from_text(text)
        if order_number:
            return order_number
    return ""


def _is_bad_contact_email(email):
    e = (email or "").strip().lower()
    if not e:
        return True
    bad_bits = [
        "noreply@rezdy.com",
        "no-reply@rezdy.com",
        "@reply.getyourguide.com",
        "message@reply.getyourguide.com",
        "@rezdy.com",
    ]
    return any(bit in e for bit in bad_bits)


def _extract_contact_info(convo, messages):
    emails = []
    phones = []

    for author in (convo.get("authors") or []):
        if author.get("address"):
            emails.extend(_extract_emails(author.get("address")))
        if author.get("name"):
            phones.extend(_extract_phones(author.get("name")))

    for key in ("subject", "latest_message_subject", "snippet", "preview", "body", "text"):
        value = convo.get(key)
        if value:
            emails.extend(_extract_emails(value))
            phones.extend(_extract_phones(value))

    for msg in messages or []:
        for key in ("subject", "preview", "body", "text"):
            value = msg.get(key)
            if value:
                emails.extend(_extract_emails(value))
                phones.extend(_extract_phones(value))

    emails = list(dict.fromkeys([e for e in emails if e and not _is_bad_contact_email(e)]))
    phones = list(dict.fromkeys([p for p in phones if p]))

    return {
        "email": emails[0] if emails else "",
        "phone": phones[0] if phones else "",
        "emails": emails,
        "phones": phones,
    }


def _find_rezdy_detail(order_number):
    if not order_number:
        return None
    try:
        booking = get_booking_by_order_number(order_number)
        if booking:
            item = (booking.get("items") or [{}])[0]
            return {
                "order_number": booking.get("orderNumber") or order_number,
                "customer_name": (booking.get("customer") or {}).get("name") or "",
                "order_email": ((booking.get("customer") or {}).get("email") or ""),
                "order_phone": ((booking.get("customer") or {}).get("phone") or ""),
                "raw_booking_json": json.dumps(booking, indent=2, default=str),
                "raw_item_json": json.dumps(item, indent=2, default=str),
            }
    except Exception:
        pass
    for day_offset in range(0, 21):
        try:
            detail = get_rezdy_order_detail(order_number, day_offset=day_offset)
            if detail:
                return detail
        except Exception:
            pass
    return None


def _parse_travelers_from_comments(text):
    if not text:
        return []

    normalized = str(text).replace("\r\n", "\n").replace("\r", "\n")
    travellers = []
    for match in TRAVELER_BLOCK_RE.finditer(normalized):
        idx = int(match.group(1))
        block = match.group(2)

        def grab(label):
            m = re.search(rf"{label}\s*:\s*(.+)", block, re.IGNORECASE)
            return m.group(1).strip() if m else ""

        first_name = grab("First Name")
        last_name = grab("Last Name")
        weight = grab("Weight")
        phone = grab("Phone")
        email = grab("Email")

        if first_name or last_name or weight or phone or email:
            travellers.append({
                "index": idx,
                "first_name": first_name,
                "last_name": last_name,
                "weight": weight,
                "phone": phone,
                "email": email,
            })

    return travellers


def _extract_customer_name(parsed, convo, messages, detail=None, travellers=None):
    parsed_name = (parsed or {}).get("customer_name", "").strip()
    if parsed_name and parsed_name.lower() != "unknown customer":
        return parsed_name

    if detail and isinstance(detail, dict):
        detail_name = (detail.get("customer_name") or "").strip()
        if detail_name and detail_name.lower() != "unknown customer":
            return detail_name

    travellers = travellers or []
    if travellers:
        lead = travellers[0]
        lead_name = f"{lead.get('first_name','').strip()} {lead.get('last_name','').strip()}".strip()
        if lead_name:
            return lead_name

    for text in _conversation_texts(convo, messages):
        m = NAME_LINE_RE.search(text)
        if m:
            candidate = re.sub(WAT_RE, "", m.group(1)).strip(" -–—|,.\n\r\t")
            candidate = re.sub(r"\s+from\s+(getyourguide|hero|viator|rezdy)$", "", candidate, flags=re.I).strip()
            if candidate and len(candidate) > 2:
                return candidate

    subject = convo.get("subject") or convo.get("latest_message_subject") or ""
    m = re.search(r"new\s+confirmed\s+order\s+(WAT[A-Z0-9]+)\s+for\s+(.+?)(?:\s+from\s+.+)?$", subject, flags=re.I)
    if m:
        return m.group(2).strip()

    return "Unknown customer"

def _participant_fields(first_name="", last_name="", email="", phone="", weight=""):
    fields = []
    if first_name:
        fields.append({"label": "First Name", "value": first_name})
    if last_name:
        fields.append({"label": "Last Name", "value": last_name})
    if phone:
        fields.append({"label": "Phone", "value": phone})
    if email:
        fields.append({"label": "Email", "value": email})
    if weight:
        fields.append({"label": "Passenger Weight", "value": weight})
    return fields


def _load_booking_json(detail):
    raw_booking_json = detail.get("raw_booking_json")
    raw_item_json = detail.get("raw_item_json")
    booking = json.loads(raw_booking_json) if raw_booking_json else {}
    item = json.loads(raw_item_json) if raw_item_json else {}
    return booking, item


def _normalize_label(label):
    return re.sub(r"[^a-z0-9]+", " ", (label or "").strip().lower()).strip()


def _participant_name_from_fields(participant):
    fields = participant.get("fields") or []
    fmap = {_normalize_label(f.get("label")): str(f.get("value", "")).strip() for f in fields if f.get("label")}
    first = fmap.get("first name", "")
    last = fmap.get("last name", "")
    return f"{first} {last}".strip()


def _find_field(fields, labels):
    labels = {_normalize_label(x) for x in labels}
    for field in fields:
        if _normalize_label(field.get("label")) in labels:
            return field
    return None


def _set_field(fields, label, value, aliases=None):
    aliases = aliases or []
    field = _find_field(fields, [label] + list(aliases))
    if field is None:
        field = {"label": label, "value": value}
        fields.append(field)
    else:
        field["label"] = field.get("label") or label
        field["value"] = value
    return fields


def _participant_match_score(participant, traveller):
    score = 0
    participant_name = _participant_name_from_fields(participant).lower()
    traveller_name = f"{traveller.get('first_name','').strip()} {traveller.get('last_name','').strip()}".strip().lower()
    if participant_name and traveller_name and participant_name == traveller_name:
        score += 100
    elif participant_name and traveller_name and participant_name.split()[:1] == traveller_name.split()[:1]:
        score += 30
    if participant.get("email") and traveller.get("email") and participant.get("email") == traveller.get("email"):
        score += 20
    return score


def _map_travellers_to_participants(existing_participants, travellers):
    if len(existing_participants) != len(travellers):
        return None
    remaining = list(range(len(existing_participants)))
    mapped = [None] * len(travellers)
    for t_idx, traveller in enumerate(travellers):
        scored = sorted(
            ((idx, _participant_match_score(existing_participants[idx], traveller)) for idx in remaining),
            key=lambda x: x[1],
            reverse=True,
        )
        chosen = scored[0][0] if scored else remaining[0]
        mapped[t_idx] = chosen
        remaining.remove(chosen)
    return mapped


def _update_participant_fields(existing_participant, traveller, fallback_email="", fallback_phone=""):
    participant = json.loads(json.dumps(existing_participant))
    fields = participant.get("fields") or []
    participant["fields"] = fields

    first_name = (traveller.get("first_name") or "").strip()
    last_name = (traveller.get("last_name") or "").strip()
    email = (traveller.get("email") or fallback_email or "").strip()
    phone = (traveller.get("phone") or fallback_phone or "").strip()
    weight = (traveller.get("weight") or "").strip()

    if first_name:
        _set_field(fields, "First Name", first_name)
    if last_name:
        _set_field(fields, "Last Name", last_name)
    if email:
        _set_field(fields, "Email", email)
    if phone:
        _set_field(fields, "Phone", phone, aliases=["Mobile"])
    if weight:
        _set_field(fields, "Passenger Weight", weight, aliases=["Weight", "Guest Weight"])

    return participant


def _merge_travellers_into_booking(booking, item, travellers, fallback_email="", fallback_phone=""):
    merged = json.loads(json.dumps(booking))
    items = merged.get("items") or []

    item_index = None
    item_code = item.get("productCode") if isinstance(item, dict) else None
    item_start = item.get("startTimeLocal") if isinstance(item, dict) else None
    item_name = item.get("productName") if isinstance(item, dict) else None

    for idx, existing in enumerate(items):
        if item_code and existing.get("productCode") == item_code:
            item_index = idx
            break
        if item_start and existing.get("startTimeLocal") == item_start:
            item_index = idx
            break
        if item_name and existing.get("productName") == item_name:
            item_index = idx
            break

    if item_index is None:
        return None, "Matching booking item not found for update"

    target_item = items[item_index]
    existing_participants = list(target_item.get("participants") or [])
    if not existing_participants:
        return None, "Rezdy booking has no participants to update"
    if len(existing_participants) != len(travellers):
        return None, f"Participant mismatch: booking has {len(existing_participants)} participant(s) but email has {len(travellers)}"

    mapping = _map_travellers_to_participants(existing_participants, travellers)
    if mapping is None:
        return None, "Could not map travellers onto the existing Rezdy participants"

    updated_participants = list(existing_participants)
    for t_idx, participant_idx in enumerate(mapping):
        updated_participants[participant_idx] = _update_participant_fields(
            existing_participants[participant_idx],
            travellers[t_idx],
            fallback_email=fallback_email if participant_idx == 0 else "",
            fallback_phone=fallback_phone if participant_idx == 0 else "",
        )

    target_item["participants"] = updated_participants
    items[item_index] = target_item
    merged["items"] = items

    booking_level_participants = list(merged.get("participants") or [])
    if booking_level_participants and len(booking_level_participants) == len(updated_participants):
        merged["participants"] = [
            updated_participants[i] if i < len(updated_participants) else booking_level_participants[i]
            for i in range(len(booking_level_participants))
        ]

    return merged, ""


def _find_text_values(obj, keys, found=None):
    found = found or []
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in {x.lower() for x in keys} and not isinstance(v, (dict, list)):
                found.append(str(v))
            _find_text_values(v, keys, found)
    elif isinstance(obj, list):
        for item in obj:
            _find_text_values(item, keys, found)
    return found


def _booking_update_eligibility(booking):
    payment_values = [v.strip().lower() for v in _find_text_values(booking, [
        "paymentType", "paymentMethod", "paymentOption", "paymentStatus", "payment", "label", "type"
    ]) if str(v).strip()]
    payments = booking.get("payments") or []
    if any("manual" in v or "cash" in v for v in payment_values):
        return True, "Manual payment marker found"
    if payments and all(str((p.get("type") or "")).upper() in {"CASH", "MANUAL", "BANK_TRANSFER", "ACCOUNT_BALANCE"} for p in payments):
        return True, "Manual-style payment entries found"
    blocked = ("credit", "card", "paypal", "stripe", "apple pay", "google pay", "online", "rezdypay")
    if any(any(bit in v for bit in blocked) for v in payment_values):
        return False, "Booking does not appear to be a manual-payment booking"
    return True, "Payment mode not explicitly exposed; attempting update"


def _rezdy_headers():
    api_key = current_app.config["REZDY_API_KEY"].strip()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json; charset=UTF-8",
        "apiKey": api_key,
    }


def _rezdy_update_booking(order_number, booking):
    base = current_app.config["REZDY_API_BASE"].rstrip("/")
    api_key = current_app.config["REZDY_API_KEY"].strip()
    url = f"{base}/bookings/{order_number}"

    try:
        resp = requests.put(
            url,
            headers=_rezdy_headers(),
            params={"apiKey": api_key},
            json=booking,
            timeout=45,
        )
        try:
            data = resp.json()
        except Exception:
            data = {"raw": resp.text}

        request_status = data.get("requestStatus", {}) if isinstance(data, dict) else {}
        return {
            "ok": resp.status_code < 300 and (not request_status or request_status.get("success", True)),
            "method": "PUT",
            "url": url,
            "status_code": resp.status_code,
            "data": data,
        }
    except Exception as exc:
        return {
            "ok": False,
            "method": "PUT",
            "url": url,
            "status_code": 500,
            "data": {"error": str(exc)},
        }


def _ops_store_path(order_number):
    base = Path(current_app.root_path).parent / "instance" / "ops" / "orders"
    base.mkdir(parents=True, exist_ok=True)
    return base / f"{order_number}.json"


def _save_order_ops_record(order_number, payload):
    path = _ops_store_path(order_number)
    data = dict(payload or {})
    data.setdefault("processed_at", datetime.now().isoformat())
    path.write_text(json.dumps(data, indent=2, default=str))
    return str(path)


from datetime import datetime



def _processed_stats():
    base = Path(current_app.root_path).parent / "instance" / "ops" / "orders"
    base.mkdir(parents=True, exist_ok=True)
    today = datetime.now().date().isoformat()
    count = 0
    for path in base.glob("*.json"):
        try:
            payload = json.loads(path.read_text())
        except Exception:
            continue
        processed_at = str(payload.get("processed_at") or "")
        if processed_at[:10] == today and payload.get("rezdy_updated"):
            count += 1
    return {"today_processed": count}

@orders.route("/")
@orders.route("")
@login_required
def inbox():
    conversations = _get_conversations()
    rows = []
    seen = set()

    for convo in conversations:
        subject = convo.get("subject") or convo.get("latest_message_subject") or "(No subject)"
        if not _is_actionable_order_subject(subject):
            continue

        parsed = _parse_subject(subject)
        unique_key = convo.get("id") or subject
        if unique_key in seen:
            continue
        seen.add(unique_key)

        messages = _get_messages(convo.get("id", "")) if convo.get("id") else []
        order_number = parsed.get("order_number") or _extract_order_number(convo, parsed, messages)
        if not order_number:
            continue

        authors = convo.get("authors") or []
        sender = _clean_sender_name(authors[0].get("name")) if authors else "Unknown"
        customer_name = _extract_customer_name(parsed, convo, messages)

        rows.append({
            "id": convo.get("id", ""),
            "subject": subject,
            "sender": sender,
            "summary_type": parsed.get("summary_type") or subject,
            "order_number": order_number,
            "customer_name": customer_name,
            "agent": parsed.get("agent") or sender or "",
            "status": parsed.get("status") or "confirmed",
            "timestamp": _format_timestamp(convo.get("last_activity_at")),
        })

    processed = _processed_stats()
    return render_template(
        "orders.html",
        rows=rows,
        total_orders=len(rows),
        new_orders=len(rows),
        cancelled_orders=0,
        processed_today=processed.get("today_processed", 0),
    )


@orders.route("/process/<conversation_id>", methods=["POST"])
@login_required
def process_order(conversation_id):
    convo = _get_conversation(conversation_id)
    if not convo:
        return jsonify({"ok": False, "error": "Conversation not found"}), 404

    subject = convo.get("subject") or convo.get("latest_message_subject") or "(No subject)"
    if not _is_actionable_order_subject(subject):
        return jsonify({"ok": False, "error": "Only New Confirmed order and Product Sold emails with a WAT number can be processed"}), 400

    parsed = _parse_subject(subject)
    messages = _get_messages(conversation_id)

    order_number = _extract_order_number(convo, parsed, messages)
    if not order_number:
        return jsonify({"ok": False, "error": "No WAT order number found in the email/conversation"}), 400

    detail = _find_rezdy_detail(order_number)
    if not detail:
        return jsonify({"ok": False, "error": f"Could not find {order_number} in Rezdy"}), 404

    booking, item = _load_booking_json(detail)
    if not booking:
        return jsonify({"ok": False, "error": f"Rezdy booking JSON missing for {order_number}"}), 500

    reseller_comments = _first_value(
        item.get("resellerComments") if isinstance(item, dict) else "",
        booking.get("resellerComments") if isinstance(booking, dict) else "",
    )

    travellers = _parse_travelers_from_comments(reseller_comments)
    if not travellers:
        return jsonify({
            "ok": False,
            "error": f"No traveler blocks found in resellerComments for {order_number}",
            "resellerComments": reseller_comments,
        }), 400

    contact = _extract_contact_info(convo, messages)
    fallback_email = detail.get("order_email", "") if isinstance(detail, dict) else ""
    fallback_phone = detail.get("order_phone", "") if isinstance(detail, dict) else ""
    if _is_bad_contact_email(contact.get("email", "")) or not contact.get("email", ""):
        contact["email"] = "" if _is_bad_contact_email(fallback_email) else fallback_email
    if not contact.get("phone", ""):
        contact["phone"] = fallback_phone

    customer_name = _extract_customer_name(parsed, convo, messages, detail=detail, travellers=travellers)
    booking_can_update, booking_update_reason = _booking_update_eligibility(booking)
    if not booking_can_update:
        saved_path = _save_order_ops_record(order_number, {
            "conversation_id": conversation_id,
            "order_number": order_number,
            "customer_name": customer_name,
            "agent": parsed.get("agent") or "",
            "subject": subject,
            "email_used": contact.get("email", ""),
            "phone_used": contact.get("phone", ""),
            "traveller_count": len(travellers),
            "travellers": travellers,
            "rezdy_updated": False,
            "eligibility_error": booking_update_reason,
        })
        return jsonify({
            "ok": False,
            "order_number": order_number,
            "customer_name": customer_name,
            "email_used": contact.get("email", ""),
            "phone_used": contact.get("phone", ""),
            "traveller_count": len(travellers),
            "travellers": travellers,
            "saved_locally": True,
            "saved_path": saved_path,
            "error": booking_update_reason,
        }), 400

    merged_booking, merge_error = _merge_travellers_into_booking(
        booking,
        item,
        travellers,
        fallback_email=contact.get("email", ""),
        fallback_phone=contact.get("phone", ""),
    )

    ops_payload = {
        "processed_at": datetime.now().isoformat(),
        "conversation_id": conversation_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "agent": parsed.get("agent") or "",
        "subject": subject,
        "email_used": contact.get("email", ""),
        "phone_used": contact.get("phone", ""),
        "traveller_count": len(travellers),
        "travellers": travellers,
        "booking_update_reason": booking_update_reason,
        "merge_error": merge_error,
        "rezdy_updated": False,
    }

    if merge_error:
        saved_path = _save_order_ops_record(order_number, ops_payload)
        return jsonify({
            "ok": False,
            "order_number": order_number,
            "customer_name": customer_name,
            "email_used": contact.get("email", ""),
            "phone_used": contact.get("phone", ""),
            "traveller_count": len(travellers),
            "travellers": travellers,
            "saved_locally": True,
            "saved_path": saved_path,
            "error": merge_error,
        }), 400

    update_result = _rezdy_update_booking(order_number, merged_booking)
    ops_payload["rezdy_updated"] = update_result.get("ok", False)
    ops_payload["rezdy_update"] = update_result
    saved_path = _save_order_ops_record(order_number, ops_payload)

    return jsonify({
        "ok": update_result.get("ok", False),
        "conversation_id": conversation_id,
        "order_number": order_number,
        "customer_name": customer_name,
        "email_used": contact.get("email", ""),
        "phone_used": contact.get("phone", ""),
        "traveller_count": len(travellers),
        "travellers": travellers,
        "saved_locally": True,
        "saved_path": saved_path,
        "rezdy_update": update_result,
        "error": "Rezdy update failed" if not update_result.get("ok") else "",
    }), (200 if update_result.get("ok") else 500)
