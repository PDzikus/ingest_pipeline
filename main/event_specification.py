"""Event related functions."""
import json
import logging
import os
from typing import Dict, Any
import jsonschema


class EventSpecification:
    """Class responsible for all functions connected with Event schema."""

    def __init__(self, schema_path: str):
        """Initialized new EventSpecification, loads schema from file.

        :param schema_path: string with the path to the schema file - as defined by JSON Schema format
                            https://json-schema.org/
        """
        logging.info("Loading schema file: %s", schema_path)
        if not os.path.isfile(schema_path):
            logging.error("Input file can't be found or accessed: %s", schema_path)
            raise FileNotFoundError
        with open(schema_path, "r") as schema_file:
            self.schema = json.load(schema_file)

    def is_event_valid(self, event: Dict[str, Any]) -> bool:
        """Validates dictionary with event object against schema."""
        try:
            jsonschema.validate(event, self.schema)
        except jsonschema.exceptions.ValidationError as error:
            logging.debug(error)
            return False
        return True

    @staticmethod
    def table_creation_sql(table_name: str) -> str:
        """Produced SQL string for PostgreSQL to create table for current event definition.

        This method is not connected to schema in any way, if schema changes, this must be applied manually.
        """
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

    @staticmethod
    def to_csv_line(event: Dict[str, Any]) -> str:
        """Maps event record to a CSV format with '|' separator.

        This method is not connected to schema in any way, if schema changes, this must be applied manually.
        """
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
