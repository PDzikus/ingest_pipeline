"""Tests for data load and validation."""
import json
from typing import Dict, Any, List

import pytest

from main.event_specification import EventSpecification


@pytest.fixture
def invalid_records() -> List[Dict[str, Any]]:
    records = []
    with open("invalid_records.json", "r") as records_file:
        for record in records_file:
            records.append(json.loads(record))
    return records


@pytest.fixture
def event_spec() -> EventSpecification:
    return EventSpecification("../main/schema/schema.json")


def test_validate_record(event_spec: EventSpecification) -> None:
    record = {
        "event_type": 1,
        "event_time": "2019-03-10 15:59:08",
        "data": {"user_email": "YINPgIQkZkh@gmail.com", "phone_number": "8923626200"},
        "processing_date": "2019-03-10",
    }
    assert event_spec.is_event_valid(record)


def test_invalid_records(
    event_spec: EventSpecification, invalid_records: List[Dict[str, Any]]
) -> None:
    for record in invalid_records:
        assert not event_spec.is_event_valid(record)
