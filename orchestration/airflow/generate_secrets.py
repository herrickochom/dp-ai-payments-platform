"""
Generate Airflow runtime secrets.

Output is shell assignment syntax intended to be copied into .env.
The values are never persisted by this program.
"""

from __future__ import annotations

import base64
import os
import secrets


def fernet_key() -> str:
    return base64.urlsafe_b64encode(os.urandom(32)).decode()


print(f"AIRFLOW_FERNET_KEY={fernet_key()}")
print(f"AIRFLOW_SECRET_KEY={secrets.token_urlsafe(48)}")
print(f"AIRFLOW_JWT_SECRET={secrets.token_urlsafe(48)}")
