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


def events_from_file(
    file_name: str, event_spec: EventSpecification
) -> Iterator[Dict[str, Any]]:
    logger = logging.getLogger("file_processor")
    with open(file_name) as source_file:
        for num, line in enumerate(source_file, 1):
            event = _process_line(line, event_spec)
            if event is None:
                logger.error("Incorrect event format in line %s: %s", num, line)
                continue
            else:
                yield event


def _process_line(
    line: str, event_spec: EventSpecification
) -> Optional[Dict[str, Any]]:
    """Line convertion to dict and validation."""
    if line is not None:
        try:
            record = json.loads(line)
        except JSONDecodeError:
            return None
        if event_spec.is_event_valid(record):
            return record
        else:
            return None
