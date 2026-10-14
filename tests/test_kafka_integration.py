"""
End-to-end against a real Kafka broker (runs in CI; locally: docker compose up -d kafka).
Producer -> topic -> consumer micro-batch -> landed files.
"""

import gzip
import json
import os

import pytest

pytestmark = pytest.mark.skipif(os.getenv("KAFKA_INTEGRATION") != "1",
                                reason="set KAFKA_INTEGRATION=1 with a broker on KAFKA_BOOTSTRAP")


def test_stream_round_trip_with_faults_and_duplicates(tmp_path):
    from streaming.consumer import consume_batch
    from streaming.producer import run

    sent = run(rate=0, limit=400, chaos=0.10, seed=3)
    summary = consume_batch(max_messages=10_000, idle_timeout=5, landing_dir=tmp_path)

    assert summary["messages"] >= sent                 # duplicates make it a bit more
    assert summary["invalid"] > 0                      # injected faults went to the DLQ file
    with gzip.open(summary["files"]["valid"], "rt") as f:
        ids = [json.loads(line)["event_id"] for line in f]
    assert len(ids) > len(set(ids))                    # at-least-once duplicates exist ...
    assert len(set(ids)) + summary["invalid"] == sent  # ... and nothing was lost

    # offsets were committed: a second batch finds nothing new
    assert consume_batch(max_messages=100, idle_timeout=3, landing_dir=tmp_path)["messages"] == 0
