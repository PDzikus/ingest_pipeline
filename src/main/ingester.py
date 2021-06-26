"""Module responsible for loading and validating records.

This should work as a generator - data source for database ingester.
# read line
# validate
# log incorrect lines, loop until end of file
# return validated record
"""
import logging
from typing import Dict, Any
import jsonschema


def is_record_valid(record: Dict[str, Any], schema: Dict[str, str]) -> bool:
    """Validates record against schema."""
    try:
        jsonschema.validate(record, schema)
    except jsonschema.exceptions.ValidationError as error:
        logging.log(error)
        return False
    return True
