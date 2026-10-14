"""Event contract: building events and rejecting bad ones."""

import json

import pytest

from streaming.schema import CATEGORIES, parse_message, to_event, validate_event


def test_sample_rows_become_valid_events(sample_rows):
    for row in sample_rows:
        assert validate_event(to_event(row)) == []


def test_personal_fields_never_leave_the_producer(sample_rows):
    event = to_event(sample_rows[0])
    for pii in ("first", "last", "street", "dob", "job", "zip"):
        assert pii not in event


def test_event_fields_are_normalised(sample_rows):
    event = to_event(sample_rows[0])
    assert not event["merchant"].startswith("fraud_")      # simulator artefact removed
    assert isinstance(event["is_fraud"], bool)
    assert event["category"] in CATEGORIES
    assert len(event["event_id"]) == 36                     # uuid4


def test_event_ids_are_unique(sample_rows):
    ids = {to_event(r)["event_id"] for r in sample_rows}
    assert len(ids) == len(sample_rows)


@pytest.mark.parametrize("change, expected", [
    ({"amount": None}, "missing field 'amount'"),
    ({"amount": "12.50"}, "'amount' has wrong type str"),
    ({"amount": True}, "'amount' has wrong type bool"),
    ({"amount": -5.0}, "amount is negative"),
    ({"category": "casino"}, "unknown category 'casino'"),
    ({"schema_version": 2}, "unsupported schema_version 2"),
    ({"event_time": "yesterday"}, "event_time is not ISO-8601"),
    ({"customer_lat": 123.0}, "customer coordinates out of range"),
])
def test_contract_violations_are_reported(sample_rows, change, expected):
    event = {**to_event(sample_rows[0]), **change}
    assert expected in validate_event(event)


def test_parse_message_handles_garbage():
    event, errors = parse_message(b'{"event_id": "broken...')
    assert event is None and errors[0].startswith("invalid JSON")
    event, errors = parse_message(json.dumps([1, 2, 3]))
    assert event is None and errors == ["event is not a JSON object"]
