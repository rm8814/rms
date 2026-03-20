"""
notifications/telegram_client.py — Thin wrapper around Telegram Bot API sendMessage.

Uses requests directly (not python-telegram-bot) to avoid async conflicts with
the existing synchronous APScheduler setup.
"""

from enum import Enum

import requests


class SendResult(Enum):
    OK      = "ok"
    BAD_ID  = "bad_id"    # HTTP 400: chat not found / invalid chat_id
    BLOCKED = "blocked"   # HTTP 403: bot blocked by user or group admin


def send_message(token: str, chat_id: str, text: str) -> SendResult:
    """
    Send a Markdown-formatted message to a Telegram chat.

    Returns SendResult.OK on success.
    Returns SendResult.BAD_ID on HTTP 400 (bad chat_id).
    Returns SendResult.BLOCKED on HTTP 403 (bot blocked).
    Raises requests.RequestException for network errors (caller handles).
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    resp = requests.post(
        url,
        json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
        timeout=10,
    )
    if resp.status_code == 400:
        return SendResult.BAD_ID
    if resp.status_code == 403:
        return SendResult.BLOCKED
    resp.raise_for_status()
    return SendResult.OK
