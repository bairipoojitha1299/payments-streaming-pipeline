"""
The event contract for the `card_transactions` topic.

Producers build events with `to_event`; consumers check every message with
`validate_event`. Anything that breaks the contract goes to the dead-letter
topic with the reason attached, instead of silently corrupting the warehouse.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone

SCHEMA_VERSION = 1

CATEGORIES = {
    "entertainment", "food_dining", "gas_transport", "grocery_net", "grocery_pos",
    "health_fitness", "home", "kids_pets", "misc_net", "misc_pos", "personal_care",
    "shopping_net", "shopping_pos", "travel",
}

# field -> expected Python type(s) after JSON decoding
REQUIRED_FIELDS = {
    "event_id": str,
    "schema_version": int,
    "event_time": str,
    "trans_num": str,
    "card_id": str,
    "merchant": str,
    "category": str,
    "amount": (int, float),
    "customer_state": str,
    "customer_lat": (int, float),
    "customer_long": (int, float),
    "merchant_lat": (int, float),
    "merchant_long": (int, float),
    "is_fraud": bool,
}


def to_event(row: dict, now: datetime | None = None) -> dict:
    """Turn one source CSV row into a Kafka event.

    Personal fields in the source (name, street, date of birth, job) are never
    put on the wire - consumers only get what they need (data minimisation).
    """
    now = now or datetime.now(timezone.utc)
    return {
        "event_id": str(uuid.uuid4()),
        "schema_version": SCHEMA_VERSION,
        "event_time": now.isoformat(timespec="milliseconds"),
        "original_txn_time": str(row["trans_date_trans_time"]),
        "trans_num": str(row["trans_num"]),
        "card_id": str(row["cc_num"]),
        "merchant": str(row["merchant"]).removeprefix("fraud_"),
        "category": str(row["category"]),
        "amount": round(float(row["amt"]), 2),
        "customer_state": str(row["state"]),
        "customer_city": str(row["city"]),
        "customer_lat": float(row["lat"]),
        "customer_long": float(row["long"]),
        "merchant_lat": float(row["merch_lat"]),
        "merchant_long": float(row["merch_long"]),
        "is_fraud": str(row["is_fraud"]) in ("1", "True", "true"),
    }


def validate_event(event: object) -> list[str]:
    """Return a list of contract violations (empty list = valid event)."""
    if not isinstance(event, dict):
        return ["event is not a JSON object"]

    errors = []
    for field, expected in REQUIRED_FIELDS.items():
        if field not in event or event[field] is None:
            errors.append(f"missing field '{field}'")
        elif expected is not bool and isinstance(event[field], bool):
            errors.append(f"'{field}' has wrong type bool")      # bool is a subclass of int
        elif not isinstance(event[field], expected):
            errors.append(f"'{field}' has wrong type {type(event[field]).__name__}")
    if errors:
        return errors

    if event["schema_version"] != SCHEMA_VERSION:
        errors.append(f"unsupported schema_version {event['schema_version']}")
    if event["amount"] < 0:
        errors.append("amount is negative")
    if event["category"] not in CATEGORIES:
        errors.append(f"unknown category '{event['category']}'")
    if not (-90 <= event["customer_lat"] <= 90 and -180 <= event["customer_long"] <= 180):
        errors.append("customer coordinates out of range")
    try:
        datetime.fromisoformat(event["event_time"])
    except ValueError:
        errors.append("event_time is not ISO-8601")
    return errors


def parse_message(raw: bytes | str) -> tuple[dict | None, list[str]]:
    """Decode a Kafka message value and validate it."""
    try:
        event = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as exc:
        return None, [f"invalid JSON: {exc.__class__.__name__}"]
    errors = validate_event(event)
    return (event if not errors else None), errors
