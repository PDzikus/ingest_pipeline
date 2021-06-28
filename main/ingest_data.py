"""Data ingestion endpoint."""
import argparse
import logging
import sys
from typing import Optional, List

from event_specification import EventSpecification
from main import file_processor
from main.postgres_loader import PostgresLoader


def main(argv: Optional[List[str]] = None) -> None:
    args = parse_arguments(argv)
    configure_logging()

    event_spec = EventSpecification(args.schema_file)
    event_iterator = file_processor.events_from_file(
        file_name=args.input_file, event_spec=event_spec
    )
    loader = PostgresLoader()

    logging.info("Loading data into PostgreSQL")
    loader.load_data(data_source=event_iterator)
    logging.info("Finished loading data into PostgresSQL")


def parse_arguments(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        dest="input_file",
        required=True,
        help="Path to newline separated json data file",
    )
    parser.add_argument(
        "--schema",
        dest="schema_file",
        required=True,
        help="Path to schema definition json.",
    )
    return parser.parse_args(argv)


def configure_logging():
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


if __name__ == "__main__":
    sys.exit(main())
