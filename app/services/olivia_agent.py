from __future__ import annotations

import re
from typing import Any, Dict, Optional

from app.services.analytics import current_operation_snapshot
from app.services.olivia_tools import (
    get_cancellation_brief,
    get_commercial_brief,
    get_draft_action_brief,
    get_forward_bookings_brief,
    get_live_context,
    get_missing_info_brief,
    get_today_ops_brief,
    get_tomorrow_risk_brief,
)


SOURCE_ALIASES = {
    "gyg": "GetYourGuide",
    "getyourguide": "GetYourGuide",
    "viator": "Viator",
    "tripadvisor": "Tripadvisor",
    "direct": "Direct",
}


class OliviaAgent:
    def parse(self, question: str) -> Dict[str, Any]:
        text = (question or "").strip()
        q = text.lower()
        days = 30
        m = re.search(r"(\d+)\s*day", q)
        if m:
            days = max(1, int(m.group(1)))
        source: Optional[str] = None
        for key, value in SOURCE_ALIASES.items():
            if key in q:
                source = value
                break
        return {
            "raw": text,
            "q": q,
            "days": days,
            "source": source,
        }

    def decide(self, parsed: Dict[str, Any]) -> str:
        q = parsed["q"]
        if not q:
            return "today_brief"
        if any(term in q for term in ["missing weight", "missing info", "missing contact", "weights", "contact details"]):
            return "missing_info"
        if any(term in q for term in ["tomorrow", "risk", "at risk"]):
            return "tomorrow_risk"
        if any(term in q for term in ["cancel", "refund"]):
            return "cancellations"
        if any(term in q for term in ["next", "upcoming", "future", "forward"]):
            return "forward"
        if any(term in q for term in ["draft", "email", "reply", "write", "message"]):
            return "draft_action"
        if any(term in q for term in ["source", "revenue", "booking", "commercial", "sales", "agent"]):
            return "commercial"
        if any(term in q for term in ["today", "ops", "brief", "issues", "right now", "priority"]):
            return "today_brief"
        return "live_context"

    def run(self, question: str) -> Dict[str, Any]:
        parsed = self.parse(question)
        intent = self.decide(parsed)
        days = parsed["days"]
        source = parsed["source"]

        if intent == "today_brief":
            payload = get_today_ops_brief()
        elif intent == "tomorrow_risk":
            payload = get_tomorrow_risk_brief()
        elif intent == "missing_info":
            payload = get_missing_info_brief("tomorrow" if "tomorrow" in parsed["q"] else "today" if "today" in parsed["q"] else None)
        elif intent == "cancellations":
            payload = get_cancellation_brief(days=days, source=source)
        elif intent == "forward":
            payload = get_forward_bookings_brief(days=days, source=source)
        elif intent == "commercial":
            payload = get_commercial_brief(days=days, source=source)
        elif intent == "draft_action":
            payload = get_draft_action_brief(parsed["raw"])
        else:
            payload = get_live_context()
            snap = payload.get("summary") or {}
            payload.update({
                "headline": "Live operations context",
                "summary": (
                    f"Right now Olivia sees {snap.get('today_orders', 0)} orders today, {snap.get('today_passengers', 0)} passengers today, "
                    f"and ${snap.get('future_revenue_30d', 0):,.0f} confirmed revenue in the next 30 days."
                ),
                "actions": [
                    "Ask for a morning ops brief.",
                    "Ask which flights tomorrow are highest risk.",
                    "Ask for missing weights or missing contact details.",
                ],
            })

        answer = self._compose_answer(intent, payload)
        return {
            "ok": True,
            "query": parsed["raw"],
            "intent": intent,
            "answer": answer,
            "payload": payload,
            "meta": {
                "days": days,
                "source": source,
            },
        }

    def starter_payload(self) -> Dict[str, Any]:
        payload = get_today_ops_brief()
        return {
            "intent": "today_brief",
            "answer": self._compose_answer("today_brief", payload),
            "payload": payload,
        }

    def _compose_answer(self, intent: str, payload: Dict[str, Any]) -> str:
        summary = payload.get("summary") or "Olivia has reviewed the current data."
        risks = payload.get("risks") or []
        actions = payload.get("actions") or []
        extras = []
        if risks:
            extras.append("Risks: " + " ".join(str(r) for r in risks[:2]))
        if actions:
            extras.append("Next: " + " ".join(str(a) for a in actions[:2]))
        if intent == "commercial" and payload.get("metrics"):
            metrics = payload.get("metrics") or []
            extras.append("Commercial: " + ", ".join(f"{m.get('label')}: {m.get('value')}" for m in metrics[:3]))
        if intent == "cancellations" and payload.get("metrics"):
            metrics = payload.get("metrics") or []
            extras.append("Cancellation view: " + ", ".join(f"{m.get('label')}: {m.get('value')}" for m in metrics[:3]))
        return " ".join([summary] + extras).strip()


_agent = OliviaAgent()


def ask_olivia(question: str) -> Dict[str, Any]:
    return _agent.run(question)


def starter_olivia_payload() -> Dict[str, Any]:
    return _agent.starter_payload()
