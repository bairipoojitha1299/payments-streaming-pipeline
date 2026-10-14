import csv
from pathlib import Path

import pytest

SAMPLE = Path(__file__).resolve().parents[1] / "data" / "sample" / "card_transactions_sample.csv"


@pytest.fixture(scope="session")
def sample_rows():
    with SAMPLE.open(newline="") as f:
        reader = csv.DictReader(f)
        return [next(reader) for _ in range(200)]


class FakeMessage:
    """Minimal stand-in for confluent_kafka.Message."""

    def __init__(self, value, partition=0, offset=0):
        self._value, self._partition, self._offset = value, partition, offset

    def value(self):
        return self._value

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def error(self):
        return None
