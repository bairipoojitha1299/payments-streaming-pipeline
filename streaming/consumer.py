"""
Consumes a micro-batch from Kafka and lands it as files for Snowflake.

    python -m streaming.consumer --max-messages 5000

Delivery guarantee: **at-least-once**.
  1. Read up to N messages (or until the topic is quiet).
  2. Write valid events to  data/landing/batch_<ts>.json.gz   (newline-delimited JSON)
     and invalid ones to    data/landing/dlq_<ts>.json.gz     (+ the DLQ topic), with the reason.
  3. Only after the files are safely on disk, commit the Kafka offsets.
A crash between 2 and 3 re-reads the same messages next time, so duplicates are
possible by design. They are removed in Snowflake by deduplicating on event_id.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from streaming.config import CONSUMER_GROUP, DLQ_TOPIC, KAFKA_BOOTSTRAP, LANDING_DIR, TOPIC
from streaming.schema import parse_message

log = logging.getLogger("consumer")


@dataclass
class Batch:
    valid: list[dict] = field(default_factory=list)
    invalid: list[dict] = field(default_factory=list)
    offsets: dict[int, tuple[int, int]] = field(default_factory=dict)   # partition -> (first, last)

    @property
    def size(self) -> int:
        return len(self.valid) + len(self.invalid)


def collect(messages, received_at: str | None = None) -> Batch:
    """Split raw Kafka messages into valid events and dead letters.

    `messages` is any iterable of objects with value()/partition()/offset()
    (real confluent_kafka Messages in production, simple fakes in tests).
    """
    received_at = received_at or datetime.now(timezone.utc).isoformat(timespec="seconds")
    batch = Batch()
    for msg in messages:
        p, o = msg.partition(), msg.offset()
        first, _ = batch.offsets.get(p, (o, o))
        batch.offsets[p] = (min(first, o), o)

        event, errors = parse_message(msg.value())
        meta = {"kafka_partition": p, "kafka_offset": o, "received_at": received_at}
        if event is not None:
            batch.valid.append({**event, **meta})
        else:
            raw = msg.value()
            batch.invalid.append({
                "raw_value": raw.decode("utf-8", "replace") if isinstance(raw, bytes) else str(raw),
                "errors": errors,
                **meta,
            })
    return batch


def write_ndjson_gz(records: list[dict], path: Path) -> Path:
    """Write records as gzipped newline-delimited JSON, atomically (tmp file + rename)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")
    tmp.replace(path)
    return path


def write_batch(batch: Batch, landing_dir: Path = LANDING_DIR, stamp: str | None = None) -> dict:
    stamp = stamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    files = {}
    if batch.valid:
        files["valid"] = str(write_ndjson_gz(batch.valid, landing_dir / f"batch_{stamp}.json.gz"))
    if batch.invalid:
        files["dlq"] = str(write_ndjson_gz(batch.invalid, landing_dir / f"dlq_{stamp}.json.gz"))
    return files


def consume_batch(max_messages: int = 5000, idle_timeout: float = 10.0,
                  landing_dir: Path = LANDING_DIR, max_seconds: float = 60.0) -> dict:
    """Read one micro-batch from Kafka, land it as files, then commit offsets.

    The batch closes when it reaches `max_messages`, when the topic is quiet for
    `idle_timeout` seconds, or after `max_seconds` - whichever comes first - so a
    busy topic can't keep a batch open forever.
    """
    from confluent_kafka import Consumer, Producer   # imported here so tests don't need a broker

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,          # we commit only after the files are written
    })
    consumer.subscribe([TOPIC])
    messages = []
    try:
        # wait for the group to assign partitions before treating silence as "no data"
        deadline = time.monotonic() + 30
        while not consumer.assignment() and time.monotonic() < deadline:
            messages.extend(m for m in consumer.consume(num_messages=500, timeout=1) if m.error() is None)
        batch_deadline = time.monotonic() + max_seconds
        while len(messages) < max_messages and time.monotonic() < batch_deadline:
            chunk = consumer.consume(num_messages=min(500, max_messages - len(messages)),
                                     timeout=min(idle_timeout, max(0.1, batch_deadline - time.monotonic())))
            chunk = [m for m in chunk if m.error() is None]
            if not chunk:
                break                         # topic is quiet: close the batch
            messages.extend(chunk)

        batch = collect(messages)
        files = write_batch(batch, landing_dir)

        if batch.invalid:                     # also publish dead letters for other consumers/alerting
            dlq = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
            for bad in batch.invalid:
                dlq.produce(DLQ_TOPIC, value=json.dumps(bad).encode())
            dlq.flush(10)

        if messages:
            consumer.commit(asynchronous=False)
    finally:
        consumer.close()

    summary = {"messages": batch.size, "valid": len(batch.valid), "invalid": len(batch.invalid),
               "files": files, "offsets": {str(k): v for k, v in batch.offsets.items()}}
    log.info("batch: %s", summary)
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--max-messages", type=int, default=5000)
    p.add_argument("--idle-timeout", type=float, default=10.0)
    p.add_argument("--max-seconds", type=float, default=60.0)
    args = p.parse_args()
    print(json.dumps(consume_batch(args.max_messages, args.idle_timeout,
                                   max_seconds=args.max_seconds), indent=2))
