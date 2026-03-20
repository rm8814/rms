"""
tests/test_exely_client.py — Unit tests for ExelyClient chunking logic
"""

import pytest
from datetime import date
from unittest.mock import MagicMock, patch, call
from ingestion.exely_client import ExelyClient, ExelyAPIError


class TestFetchServicesChunked:
    def setup_method(self):
        self.client = ExelyClient(api_key="test-key", property_id="p1")
        self.client.session = MagicMock()

    def _mock_response(self, data=None):
        resp = MagicMock()
        resp.status_code = 200
        resp.json.return_value = {"data": data or {"services": [], "reservations": [], "agents": [], "roomTypes": []}}
        return resp

    def test_single_chunk_under_31_days(self):
        self.client.session.get.return_value = self._mock_response()
        start = date(2024, 3, 1)
        end = date(2024, 3, 20)   # 19 days — one chunk
        with patch("time.sleep"):
            result = self.client.fetch_services_chunked(start, end)
        assert self.client.session.get.call_count == 1

    def test_two_chunks_for_32_days(self):
        self.client.session.get.return_value = self._mock_response()
        start = date(2024, 3, 1)
        end = date(2024, 4, 1)    # 31 days = exactly 31 → 1 chunk; 32 days → 2 chunks
        with patch("time.sleep"):
            result = self.client.fetch_services_chunked(start, end)
        assert self.client.session.get.call_count == 2

    def test_results_merged_across_chunks(self):
        def side_effect(url, params, timeout):
            resp = MagicMock()
            resp.status_code = 200
            resp.json.return_value = {"data": {
                "services": [{"id": params["startDate"]}],
                "reservations": [], "agents": [], "roomTypes": []
            }}
            return resp
        self.client.session.get.side_effect = side_effect

        start = date(2024, 3, 1)
        end = date(2024, 4, 1)
        with patch("time.sleep"):
            result = self.client.fetch_services_chunked(start, end)
        assert len(result["services"]) == 2

    def test_date_range_validation_raises_on_negative(self):
        with pytest.raises(ValueError, match="start_date must be before end_date"):
            self.client.get_services(date(2024, 3, 15), date(2024, 3, 10))

    def test_401_raises_exely_api_error(self):
        resp = MagicMock()
        resp.status_code = 401
        self.client.session.get.return_value = resp
        with pytest.raises(ExelyAPIError) as exc:
            self.client.get_services(date(2024, 3, 1), date(2024, 3, 10))
        assert exc.value.status_code == 401
