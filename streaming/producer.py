"""
Replays card transactions into Kafka as a live stream.

    python -m streaming.producer --rate 20             # 20 events/second, forever (loops the file)
    python -m streaming.producer --rate 50 --limit 1000 --chaos 0.02

Messages are keyed by card_id, so every transaction of a card lands in the
same partition and stays in order. `--chaos` injects a share of broken and
duplicate messages to prove the pipeline handles them (DLQ + dedup).
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import time
from itertools import cycle

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from streaming.config import DLQ_TOPIC, KAFKA_BOOTSTRAP, SOURCE_CSV, TOPIC
from streaming.chaos import corrupt
from streaming.schema import to_event

log = logging.getLogger("producer")


def ensure_topics(bootstrap: str, partitions: int = 3) -> None:
    """Create the topics if they don't exist (safe to call repeatedly)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    existing = admin.list_topics(timeout=10).topics
    new = [NewTopic(t, num_partitions=partitions, replication_factor=1)
           for t in (TOPIC, DLQ_TOPIC) if t not in existing]
    if not new:
        return
    for topic, future in admin.create_topics(new).items():
        future.result()
        log.info("created topic %s", topic)


def run(rate: float, limit: int | None, chaos: float, seed: int = 7) -> int:
    ensure_topics(KAFKA_BOOTSTRAP)
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "enable.idempotence": True,      # no broker-side duplicates on retries
        "acks": "all",
        "linger.ms": 50,
        "compression.type": "lz4",
    })
    rng = random.Random(seed)
    sent = 0
    delay = 1.0 / rate if rate > 0 else 0

    with SOURCE_CSV.open(newline="") as f:
        rows = list(csv.DictReader(f))
    log.info("streaming %s rows from %s at %.0f events/s (chaos=%.0f%%)",
             f"{len(rows):,}", SOURCE_CSV, rate, chaos * 100)

    for row in cycle(rows):
        event = to_event(row)
        key = event["card_id"].encode()
        roll = rng.random()
        if roll < chaos / 2:
            producer.produce(TOPIC, key=key, value=corrupt(event, rng))
        else:
            value = json.dumps(event).encode()
            producer.produce(TOPIC, key=key, value=value)
            if roll < chaos:   # simulate an upstream retry: same event sent twice
                producer.produce(TOPIC, key=key, value=value)
        sent += 1
        producer.poll(0)
        if sent % 1000 == 0:
            log.info("sent %s events", f"{sent:,}")
        if limit and sent >= limit:
            break
        if delay:
            time.sleep(delay)

    producer.flush(30)
    log.info("done: %s events sent", f"{sent:,}")
    return sent


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--rate", type=float, default=20, help="events per second (0 = as fast as possible)")
    p.add_argument("--limit", type=int, default=None, help="stop after N events (default: run forever)")
    p.add_argument("--chaos", type=float, default=0.02, help="share of bad/duplicate messages to inject")
    args = p.parse_args()
    run(args.rate, args.limit, args.chaos)
