"""Tests for data load and validation."""
import json
from typing import Dict

import pytest

from main.event_tools import is_event_valid

record = {
    "event_type": 1,
    "event_time": "2019-03-10 15:59:08",
    "data": {"user_email": "YINPgIQkZkh@gmail.com", "phone_number": "8923626200"},
    "processing_date": "2019-03-10",
}


@pytest.fixture
def schema() -> Dict[str, str]:
    with open("../data/schema.json", "r") as schema_file:
        schema = json.load(schema_file)
    return schema


def test_validate_record(schema: Dict[str, str]) -> None:
    assert is_event_valid(record, schema)
