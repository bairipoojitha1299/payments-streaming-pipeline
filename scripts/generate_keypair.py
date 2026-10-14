"""
Generate the RSA key pair the pipeline uses to log in to Snowflake.

    docker compose run --rm airflow python /opt/airflow/scripts/generate_keypair.py

Writes keys/rsa_key.p8 (private - never commit it; keys/ is git-ignored)
and prints the ALTER USER statement with the public key to run in Snowflake.
"""

from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

KEY_DIR = Path(__file__).resolve().parents[1] / "keys"
KEY_DIR.mkdir(exist_ok=True)
private_path = KEY_DIR / "rsa_key.p8"

if private_path.exists():
    raise SystemExit(f"{private_path} already exists - delete it first if you really want a new key.")

key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
private_path.write_bytes(key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
))
public_pem = key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode()
public_body = "".join(line for line in public_pem.splitlines() if "PUBLIC KEY" not in line)

print(f"Private key written to {private_path}\n")
print("Run this in Snowflake (as ACCOUNTADMIN):\n")
print(f"ALTER USER PIPELINE_SVC SET RSA_PUBLIC_KEY = '{public_body}';")
