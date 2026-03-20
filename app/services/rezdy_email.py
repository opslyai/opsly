def _walk(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for item in obj:
            yield from _walk(item)

def extract_customer_email(order):
    if not isinstance(order, dict):
        return ""

    direct_keys = [
        "email",
        "customerEmail",
        "customer_email",
        "contactEmail",
        "contact_email",
        "leadEmail",
        "lead_email",
        "billingEmail",
        "billing_email",
    ]

    for key in direct_keys:
        value = order.get(key)
        if value and "@" in str(value):
            return str(value).strip()

    for node in _walk(order):
        if not isinstance(node, dict):
            continue
        for key in direct_keys:
            value = node.get(key)
            if value and "@" in str(value):
                return str(value).strip()

    for node in _walk(order):
        if not isinstance(node, dict):
            continue
        for k, v in node.items():
            if isinstance(v, str) and "@" in v:
                lk = str(k).lower()
                if "email" in lk:
                    return v.strip()

    return ""

def extract_customer_name(order):
    if not isinstance(order, dict):
        return "Customer"

    candidate_paths = [
        ("customerName",),
        ("customer_name",),
        ("name",),
        ("customer", "name"),
        ("customer", "fullName"),
        ("contact", "name"),
        ("leadCustomer", "name"),
    ]

    for path in candidate_paths:
        current = order
        ok = True
        for part in path:
            if isinstance(current, dict) and part in current and current[part]:
                current = current[part]
            else:
                ok = False
                break
        if ok and isinstance(current, str):
            return current.strip()

    return "Customer"
