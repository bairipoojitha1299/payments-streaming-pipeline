"""Micro-batch logic: splitting good/bad messages, landing files, loader bookkeeping."""

import gzip
import json
import random

from conftest import FakeMessage
from streaming.chaos import corrupt
from streaming.consumer import collect, write_batch
from streaming.schema import parse_message, to_event
from streaming.snowflake_loader import copy_sql, pending_files


def test_every_kind_of_injected_fault_is_rejected(sample_rows):
    rng = random.Random(0)
    for i in range(300):
        bad = corrupt(to_event(sample_rows[i % len(sample_rows)]), rng)
        event, errors = parse_message(bad)
        assert event is None and errors, bad


def test_collect_splits_valid_and_dead_letters(sample_rows):
    good = [json.dumps(to_event(r)).encode() for r in sample_rows[:5]]
    msgs = [FakeMessage(v, partition=i % 2, offset=10 + i) for i, v in enumerate(good)]
    msgs.append(FakeMessage(b"not json", partition=1, offset=99))

    batch = collect(msgs, received_at="2026-01-01T00:00:00+00:00")

    assert len(batch.valid) == 5 and len(batch.invalid) == 1
    assert batch.invalid[0]["raw_value"] == "not json"
    assert batch.invalid[0]["errors"][0].startswith("invalid JSON")
    assert batch.valid[0]["kafka_partition"] == 0 and batch.valid[0]["kafka_offset"] == 10
    assert batch.offsets == {0: (10, 14), 1: (11, 99)}


def test_write_batch_lands_gzipped_ndjson(tmp_path, sample_rows):
    msgs = [FakeMessage(json.dumps(to_event(r)).encode(), offset=i) for i, r in enumerate(sample_rows[:20])]
    msgs.append(FakeMessage(b'{"amount": -1}', offset=20))
    files = write_batch(collect(msgs), tmp_path, stamp="20260101T000000Z")

    with gzip.open(files["valid"], "rt") as f:
        lines = [json.loads(line) for line in f]
    assert len(lines) == 20 and lines[0]["kafka_offset"] == 0
    with gzip.open(files["dlq"], "rt") as f:
        assert len(f.readlines()) == 1
    assert not list(tmp_path.glob("*.tmp"))            # atomic write leaves no temp files


def test_empty_batch_writes_nothing(tmp_path):
    assert write_batch(collect([]), tmp_path) == {}
    assert not list(tmp_path.iterdir())


def test_pending_files_are_routed_to_the_right_table(tmp_path):
    for name in ["batch_2.json.gz", "dlq_1.json.gz", "batch_1.json.gz", "notes.txt"]:
        (tmp_path / name).write_bytes(b"")
    routed = [(p.name, t) for p, t in pending_files(tmp_path)]
    assert routed == [("batch_1.json.gz", "RAW.CARD_TRANSACTIONS"),
                      ("batch_2.json.gz", "RAW.CARD_TRANSACTIONS"),
                      ("dlq_1.json.gz", "RAW.CARD_TRANSACTIONS_DLQ")]


def test_copy_statement_loads_only_the_named_file():
    sql = copy_sql("RAW.CARD_TRANSACTIONS", "batch_1.json.gz")
    assert "FILES = ('batch_1.json.gz')" in sql
    assert "METADATA$FILENAME" in sql and "ON_ERROR = 'ABORT_STATEMENT'" in sql
