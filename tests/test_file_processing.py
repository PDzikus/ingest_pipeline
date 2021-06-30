"""Tests for file processing."""
from models.event import Event
from source_processor import SourceProcessor
from datetime import datetime, date

records = [
    Event(
        7,
        datetime(2019, 1, 4, 5, 7, 26),
        "TAFgZ@gmail.com",
        "0603728764",
        date(2019, 1, 4),
    ),
    Event(
        6,
        datetime(2018, 1, 4, 22, 25, 53),
        "RNRKlh@gmail.com",
        "5164649899",
        date(2018, 1, 4),
    ),
    Event(
        3,
        datetime(2018, 1, 8, 16, 27, 50),
        "fiXNIY@gmail.com",
        "1432631330",
        date(2018, 1, 8),
    ),
    Event(
        7,
        datetime(2019, 1, 14, 18, 49, 24),
        "PAjIeMQlnf@gmail.com",
        "4734922788",
        date(2019, 1, 14),
    ),
]


def test_file_iterator():
    file_path = "test_data/mixed_data.json"
    source_processor = SourceProcessor()
    file_iterator = source_processor.iterator_from_file(file_path)
    output = list(file_iterator)
    assert len(output) == 4
    for record in output:
        assert record in records
