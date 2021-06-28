"""Data ingestion endpoint."""
import argparse
import logging
import os
import sys
from typing import Optional, List

from event_specification import EventSpecification
from source_processor import SourceProcessor
from main.postgres_loader import PostgresLoader


def main(argv: Optional[List[str]] = None) -> None:
    configure_logging()
    logger = logging.getLogger("Main")
    args = parse_arguments(argv)
    validate_args(args)

    event_spec = EventSpecification(args.schema_file)
    source_processor = SourceProcessor(event_spec)
    event_iterator = source_processor.iterator_from_file(args.input_file)
    db_loader = PostgresLoader()

    logger.info("Loading data into PostgreSQL")
    db_loader.load_data(data_source=event_iterator)
    logger.info("Finished loading data into PostgresSQL")


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
        default="schema/schema.json",
        required=False,
        help="Path to schema definition json.",
    )
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    logger = logging.getLogger("Main")
    if not os.path.isfile(args.input_file):
        logger.error("Input file can't be found or accessed: %s", args.input_file)
        raise FileNotFoundError
    if not os.path.isfile(args.schema_file):
        logger.error("Input file can't be found or accessed: %s", args.schema_file)
        raise FileNotFoundError


def configure_logging():
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)


if __name__ == "__main__":
    sys.exit(main())
