"""Fault injection: realistic bad messages for testing the pipeline's defences."""

from __future__ import annotations

import json
import random


def corrupt(event: dict, rng: random.Random) -> bytes:
    """Produce one of several realistic kinds of bad message."""
    kind = rng.choice(["missing_field", "negative_amount", "bad_category", "not_json"])
    bad = dict(event)
    if kind == "missing_field":
        bad.pop(rng.choice(["amount", "card_id", "event_time"]))
    elif kind == "negative_amount":
        bad["amount"] = -abs(bad["amount"]) - 1.0
    elif kind == "bad_category":
        bad["category"] = "unknown_category"
    else:
        return b'{"event_id": "broken...'
    return json.dumps(bad).encode()
