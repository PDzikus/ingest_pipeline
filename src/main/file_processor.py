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
import jsonschema


def is_record_valid(record: Dict[str, Any], schema: Dict[str, str]) -> bool:
    """Validates record against schema."""
    try:
        jsonschema.validate(record, schema)
    except jsonschema.exceptions.ValidationError as error:
        logging.getLogger("schema").debug(error)
        return False
    return True


def data_from_file(file_name: str, schema: Dict[str, str]) -> Iterator[Dict[str, Any]]:
    logger = logging.getLogger("file_processor")
    with open(file_name) as file:
        for num, line in enumerate(file, 1):
            row = process_line(line, schema)
            if row is None:
                logger.error("Incorrect format in line %s: %s", num, line)
                continue
            else:
                yield row


def process_line(line: str, schema: Dict[str, str]) -> Optional[Dict[str, Any]]:
    """Line convertion to dict and validation."""
    if line is not None:
        try:
            record = json.loads(line)
        except JSONDecodeError:
            return None
        if is_record_valid(record, schema):
            return record
        else:
            return None
