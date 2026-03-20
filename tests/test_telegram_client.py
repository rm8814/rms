"""
tests/test_telegram_client.py — Unit tests for notifications/telegram_client.py
"""

import pytest
import requests
from unittest.mock import patch, MagicMock
from notifications.telegram_client import send_message, SendResult


def _mock_response(status_code):
    resp = MagicMock()
    resp.status_code = status_code
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


class TestSendMessage:
    def test_http_200_returns_ok(self):
        with patch("notifications.telegram_client.requests.post") as mock_post:
            mock_post.return_value = _mock_response(200)
            result = send_message("TOKEN", "12345", "hello")
        assert result == SendResult.OK

    def test_http_400_returns_bad_id(self):
        with patch("notifications.telegram_client.requests.post") as mock_post:
            mock_post.return_value = _mock_response(400)
            result = send_message("TOKEN", "bad_id", "hello")
        assert result == SendResult.BAD_ID

    def test_http_403_returns_blocked(self):
        with patch("notifications.telegram_client.requests.post") as mock_post:
            mock_post.return_value = _mock_response(403)
            result = send_message("TOKEN", "12345", "hello")
        assert result == SendResult.BLOCKED

    def test_network_error_raises(self):
        with patch("notifications.telegram_client.requests.post") as mock_post:
            mock_post.side_effect = requests.ConnectionError("timeout")
            with pytest.raises(requests.ConnectionError):
                send_message("TOKEN", "12345", "hello")

    def test_http_429_raises_via_raise_for_status(self):
        with patch("notifications.telegram_client.requests.post") as mock_post:
            mock_post.return_value = _mock_response(429)
            with pytest.raises(requests.HTTPError):
                send_message("TOKEN", "12345", "hello")

    def test_sends_correct_payload(self):
        with patch("notifications.telegram_client.requests.post") as mock_post:
            mock_post.return_value = _mock_response(200)
            send_message("mytoken", "-100123", "test msg")
        call_kwargs = mock_post.call_args
        assert call_kwargs[1]["json"]["chat_id"] == "-100123"
        assert call_kwargs[1]["json"]["text"] == "test msg"
        assert call_kwargs[1]["json"]["parse_mode"] == "Markdown"
        assert "botmytoken" in call_kwargs[0][0]
