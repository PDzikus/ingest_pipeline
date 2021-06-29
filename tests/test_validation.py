"""Tests for data load and validation."""
import json
from typing import Dict, Any, List
import pytest
from models.event import Event
from datetime import datetime


@pytest.fixture
def invalid_records() -> List[Dict[str, Any]]:
    records = []
    with open("invalid_records.json", "r") as records_file:
        for record in records_file:
            records.append(json.loads(record))
    return records


def test_validate_record() -> None:
    record = {
        "event_type": 1,
        "event_time": "2019-03-10 15:59:08",
        "data": {"user_email": "YINPgIQkZkh@gmail.com", "phone_number": "8923626200"},
        "processing_date": "2019-03-10",
    }
    result = Event.from_json_object(record)
    assert result is not None
    assert result.event_type == 1
    assert result.event_time == datetime.strptime(
        "2019-03-10 15:59:08", "%Y-%m-%d %H:%M:%S"
    )
    assert result.user_email == "YINPgIQkZkh@gmail.com"
    assert result.phone_number == "8923626200"
    assert result.processing_date == datetime.strptime("2019-03-10", "%Y-%m-%d").date()


def test_invalid_records(invalid_records: List[Dict[str, Any]]) -> None:
    for record in invalid_records:
        assert Event.from_json_object(record) is None
