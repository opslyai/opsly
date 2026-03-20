import os
import json
from app.services.rezdy_email import extract_customer_email, extract_customer_name

SEARCH_DIRS = [
    os.path.expanduser("~/opsly"),
    os.path.expanduser("~/opsly/data"),
    os.path.expanduser("~/opsly/instance"),
]

ORDER_KEYS = [
    "orderNumber", "order_number", "orderNo", "order_id", "orderId",
    "bookingNumber", "booking_number", "reference", "resNo", "res_no"
]

def _walk(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk(item)

def _match_order(d, order_number):
    wanted = str(order_number).strip()
    for key in ORDER_KEYS:
        value = d.get(key)
        if value is not None and str(value).strip() == wanted:
            return True
    return False

def find_customer_by_order(order_number):
    wanted = str(order_number).strip()

    for base in SEARCH_DIRS:
        if not os.path.exists(base):
            continue

        for root, _, files in os.walk(base):
            for file in files:
                if not file.endswith(".json"):
                    continue

                path = os.path.join(root, file)

                try:
                    with open(path, "r", encoding="utf-8") as f:
                        data = json.load(f)
                except Exception:
                    continue

                for item in _walk(data):
                    if isinstance(item, dict) and _match_order(item, wanted):
                        email = extract_customer_email(item)
                        name = extract_customer_name(item)
                        return {
                            "order_number": wanted,
                            "email": email,
                            "name": name,
                            "source": path,
                        }

    return None
