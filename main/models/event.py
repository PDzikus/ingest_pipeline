"""Event models class implementation."""
from __future__ import annotations

import logging
import re
from datetime import datetime, date
from typing import Any, Dict, Optional

from jinja2 import Environment, PackageLoader

import models


class Event:
    """Class responsible for serialization, validation and deserialization of events."""

    required_event_keys = ["event_type", "event_time", "processing_date", "data"]
    required_data_keys = ["user_email", "phone_number"]

    def __init__(
        self,
        event_type: int,
        event_time: datetime,
        user_email: str,
        phone_number: str,
        processing_date: date,
    ):
        self.event_type = event_type
        self.event_time = event_time
        self.user_email = user_email
        self.phone_number = phone_number
        self.processing_date = processing_date

    @classmethod
    def from_json_object(cls, json: Dict[str, Any]) -> Optional[Event]:
        """Validates json object and creates Event object out of it, returns None if Event can't be created."""
        if not Event._json_contains_all_fields(json):
            return None
        try:
            event_type = Event._parse_event_type(json.get("event_type"))
            event_time = Event._parse_event_time(json.get("event_time"))
            user_email = Event._parse_user_email(json.get("data").get("user_email"))
            phone_number = Event._parse_phone_number(
                json.get("data").get("phone_number")
            )
            processing_date = Event._parse_processing_date(json.get("processing_date"))
        except ValueError as error:
            logging.debug(error)
            return None
        return cls(event_type, event_time, user_email, phone_number, processing_date)

    @staticmethod
    def _validate_class(value, clazz) -> None:
        if not isinstance(value, clazz):
            raise ValueError(
                f"Parse event type: Expected {clazz} but got {type(value)}"
            )

    @staticmethod
    def _parse_event_type(value: int) -> int:
        Event._validate_class(value, int)
        return value

    @staticmethod
    def _parse_event_time(value: str) -> datetime:
        Event._validate_class(value, str)
        return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _parse_user_email(value: str) -> str:
        Event._validate_class(value, str)
        email_format = re.compile("^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$")
        if re.match(email_format, value) is None:
            raise ValueError(f"Parse user_email, did not match regex. Value: {value}")
        return value

    @staticmethod
    def _parse_phone_number(value: str) -> str:
        Event._validate_class(value, str)
        email_format = re.compile("^\\d+$")
        if re.match(email_format, value) is None:
            raise ValueError(f"Parse phone_number, did not match regex. Value: {value}")
        return value

    @staticmethod
    def _parse_processing_date(value: str) -> date:
        Event._validate_class(value, str)
        return datetime.strptime(value, "%Y-%m-%d").date()

    @staticmethod
    def _json_contains_all_fields(json: Dict[str, Any]) -> bool:
        for key in Event.required_event_keys:
            if json.get(key) is None:
                logging.debug("Missing key: %s in json: %s", key, json)
                return False

        for key in Event.required_data_keys:
            if json.get("data").get(key) is None:
                logging.debug("Missing key: data.%s in json: %s", key, json)
                return False
        return True

    def to_csv_row(self):
        """Serializes Event object into csv-like row string."""
        return (
            "|".join(
                [
                    str(self.event_type),
                    self.event_time.strftime("%Y-%m-%d %H:%M:%S"),
                    self.user_email,
                    self.phone_number,
                    self.processing_date.strftime("%Y-%m-%d"),
                ]
            )
            + "\n"
        )

    @staticmethod
    def staging_table_creation_sql(table_name: str) -> str:
        """Produced SQL string for PostgreSQL to create table for current event definition.

        This method is not connected to schema in any way, if schema changes, this change must be applied manually.
        """
        jinja_env = Environment(
            loader=PackageLoader(models.__package__, "templates"),
            trim_blocks=True,
            autoescape=False,
        )
        template = jinja_env.get_template("event_staging_table.sql")
        return template.render(table_name=table_name)

    def __eq__(self, other: Event) -> bool:
        """Two events are equal if all their fields are equal."""
        return (
            self.event_type == other.event_type
            and self.event_time == other.event_time
            and self.processing_date == other.processing_date
            and self.phone_number == other.phone_number
            and self.user_email == other.user_email
        )
