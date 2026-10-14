"""Settings, read from environment variables (see .env.example)."""

import os
from pathlib import Path

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "card_transactions")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "card_transactions_dlq")
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "snowflake-loader")

LANDING_DIR = Path(os.getenv("LANDING_DIR", "data/landing"))
SOURCE_CSV = Path(os.getenv("SOURCE_CSV", "data/sample/card_transactions_sample.csv"))

# Snowflake (key-pair authentication - no passwords)
SNOWFLAKE = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
    "user": os.getenv("SNOWFLAKE_USER", "PIPELINE_SVC"),
    "private_key_file": os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "keys/rsa_key.p8"),
    "role": os.getenv("SNOWFLAKE_ROLE", "PIPELINE_ROLE"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "PIPELINE_WH"),
    "database": os.getenv("SNOWFLAKE_DATABASE", "PAYMENTS"),
    "schema": "RAW",
}
