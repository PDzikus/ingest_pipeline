"""Module responsible for loading and validating records.

This should work as a generator as a data source for database loader.
# read line
# validate
# log incorrect lines, loop until end of file
# return validated record
"""
import json
import logging
from json import JSONDecodeError
from typing import Iterator, Optional
from models.event import Event


class SourceProcessor:
    """Class delivering iterators for event objects (dict)."""

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.records_count = 0

    def iterator_from_file(self, file_name: str) -> Iterator[Event]:
        """Creates iterator of event dict objects read from file."""
        self.records_count = 0
        with open(file_name) as source_file:
            for num, line in enumerate(source_file, 1):
                event = self._process_line(line)
                if event is None:
                    self.logger.error(
                        "Incorrect event format in line %s: %s", num, line
                    )
                    continue
                else:
                    self.records_count = self.records_count + 1
                    yield event

    def _process_line(self, line: str) -> Optional[Event]:
        """Converts string to json and maps it to Even class."""
        if line is not None:
            try:
                record = json.loads(line)
            except JSONDecodeError:
                return None
            return Event.from_json_object(record)
