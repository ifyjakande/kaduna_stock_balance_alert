#!/usr/bin/env python3
"""Utilities for decrypting and summarizing failed webhook state data."""

from __future__ import annotations

import os
import pickle
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet


FAILED_WEBHOOKS_PATH = Path("encrypted_states/failed_webhooks.enc")
OUTPUT_PATH = Path("failed_webhooks_readable.txt")


def format_webhook(webhook: dict[str, Any]) -> str:
    lines = [
        f"Timestamp: {webhook.get('timestamp', 'Unknown')}",
        f"Error: {webhook.get('error', 'Unknown')}",
        f"Attempts: {webhook.get('attempts', 'Unknown')}",
        "---",
    ]
    return "\n".join(lines)


def main() -> None:
    try:
        key = os.environ["STATE_ENCRYPTION_KEY"].encode()
    except KeyError:
        print("ERROR:STATE_ENCRYPTION_KEY not set")
        return

    try:
        encrypted_data = FAILED_WEBHOOKS_PATH.read_bytes()
        decrypted = Fernet(key).decrypt(encrypted_data)
        failed_webhooks = pickle.loads(decrypted)
    except Exception as exc:  # pragma: no cover - best effort logging only
        print(f"ERROR:{exc}")
        return

    if isinstance(failed_webhooks, list) and failed_webhooks:
        OUTPUT_PATH.write_text(
            "\n".join(format_webhook(item) for item in failed_webhooks),
            encoding="utf-8",
        )
        print(f"FOUND:{len(failed_webhooks)}")
        return

    print("NONE")


if __name__ == "__main__":
    main()
