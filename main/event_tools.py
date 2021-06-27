"""Event related functions."""
import logging
from typing import Dict, Any
import jsonschema


def table_creation_schema(table_name: str) -> str:
    return f"""DROP TABLE IF EXISTS {table_name};
              CREATE UNLOGGED TABLE {table_name} (
                  event_type          INTEGER,
                  event_time          TIMESTAMP,
                  user_email          TEXT,
                  phone_number        TEXT,
                  processing_date     DATE,
                  PRIMARY KEY (event_type, event_time, user_email, phone_number)
              );
            """


def is_event_valid(event: Dict[str, Any], schema: Dict[str, str]) -> bool:
    """Validates record against schema."""
    try:
        jsonschema.validate(event, schema)
    except jsonschema.exceptions.ValidationError as error:
        logging.getLogger("schema").debug(error)
        return False
    return True


def event_to_csv_line(event: Dict[str, Any]) -> str:
    """Maps event record to a CSV format with '|' separator."""
    return (
        "|".join(
            [
                str(event["event_type"]),
                event["event_time"],
                event["data"]["user_email"],
                event["data"]["phone_number"],
                event["processing_date"],
            ]
        )
        + "\n"
    )
