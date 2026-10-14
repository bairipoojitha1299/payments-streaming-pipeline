"""
Micro-batch pipeline: Kafka -> Snowflake -> dbt, every 5 minutes.

    consume_kafka_batch  ->  load_to_snowflake  ->  dbt_source_freshness  ->  dbt_build

* consume_kafka_batch:   reads up to 10,000 new events, validates them against the
                         event contract, lands valid/invalid files, commits offsets.
* load_to_snowflake:     PUT + COPY INTO every pending file (idempotent, retries safely).
* dbt_source_freshness:  fails loudly if no new data has reached Snowflake recently.
* dbt_build:             incremental staging + marts, then all data tests.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

DBT = "/home/airflow/dbt_venv/bin/dbt"
DBT_DIR = "/opt/airflow/dbt/payments"
DBT_ARGS = f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"


@dag(
    dag_id="payments_streaming",
    description="Kafka card transactions -> Snowflake RAW -> dbt marts",
    schedule="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,               # batches must not overlap
    default_args={"owner": "poojitha", "retries": 2, "retry_delay": timedelta(minutes=1)},
    tags=["kafka", "snowflake", "dbt", "streaming"],
)
def payments_streaming():

    @task
    def consume_kafka_batch() -> dict:
        from streaming.consumer import consume_batch
        return consume_batch(max_messages=10_000, idle_timeout=10)

    @task
    def load_to_snowflake(batch: dict) -> dict:
        # loads ALL pending files, so a file left behind by a failed run is picked up here
        from streaming.snowflake_loader import load_pending
        return load_pending()

    freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} source freshness {DBT_ARGS}",
    )

    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build {DBT_ARGS}",
    )

    load_to_snowflake(consume_kafka_batch()) >> freshness >> build


payments_streaming()
