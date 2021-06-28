"""Tests for file processing."""
import pytest

from event_specification import EventSpecification
from main.file_processor import events_from_file

records = [
    {
        "event_type": 7,
        "event_time": "2019-01-04 05:07:26",
        "data": {"user_email": "TAFgZ@gmail.com", "phone_number": "0603728764"},
        "processing_date": "2019-01-04",
    },
    {
        "event_type": 6,
        "event_time": "2018-01-04 22:25:53",
        "data": {"user_email": "RNRKlh@gmail.com", "phone_number": "5164649899"},
        "processing_date": "2018-01-04",
    },
    {
        "event_type": 3,
        "event_time": "2018-01-08 16:27:50",
        "data": {"user_email": "fiXNIY@gmail.com", "phone_number": "1432631330"},
        "processing_date": "2018-01-08",
    },
    {
        "event_type": 7,
        "event_time": "2019-01-14 18:49:24",
        "data": {"user_email": "PAjIeMQlnf@gmail.com", "phone_number": "4734922788"},
        "processing_date": "2019-01-14",
    },
]


@pytest.fixture
def event_spec() -> EventSpecification:
    return EventSpecification("../main/schema/schema.json")


def test_file_iterator(event_spec: EventSpecification):
    file_path = "testing_data.json"
    file_iterator = events_from_file(file_path, event_spec)
    output = list(file_iterator)
    assert len(output) == 4
    for record in output:
        assert record in records
