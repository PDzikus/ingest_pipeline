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
from typing import Dict, Any, Iterator, Optional
from event_specification import EventSpecification


class SourceProcessor:
    """Class delivering iterators for event objects (dict)."""

    def __init__(self, event_spec: EventSpecification):
        self.event_spec = event_spec
        self.logger = logging.getLogger(self.__class__.__name__)

    def iterator_from_file(self, file_name: str) -> Iterator[Dict[str, Any]]:
        """Creates iterator of event dict objects read from file."""
        with open(file_name) as source_file:
            for num, line in enumerate(source_file, 1):
                event = self._process_line(line)
                if event is None:
                    self.logger.error(
                        "Incorrect event format in line %s: %s", num, line
                    )
                    continue
                else:
                    yield event

    def _process_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Converts string to dict and validates it against event specification."""
        if line is not None:
            try:
                record = json.loads(line)
            except JSONDecodeError:
                return None
            if self.event_spec.is_event_valid(record):
                return record
            else:
                return None
