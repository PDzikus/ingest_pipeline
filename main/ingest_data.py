"""Data ingestion endpoint."""
import argparse
import json
import logging
import os.path
import sys
from typing import Optional, List, Dict, Any

from main import file_processor
from main.postgres_loader import PostgresLoader


def main(argv: Optional[List[str]] = None) -> None:
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    logger = logging.getLogger("ingest_data")

    args = parse_arguments(argv)
    schema = load_schema(args.schema_file)
    if not os.path.isfile(args.input_file):
        logger.error("Input file can't be accessed: %s", args.input_file)
        exit(1)
    event_iterator = file_processor.events_from_file(
        file_name=args.input_file, schema=schema
    )
    loader = PostgresLoader()
    logger.info("Loading data into PostgreSQL")
    loader.load_data(data_source=event_iterator)
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
        required=True,
        help="Path to schema definition json.",
    )
    return parser.parse_args(argv)


def load_schema(schema_path: str) -> Dict[str, Any]:
    logger = logging.getLogger("ingest_data")
    logger.info("Loading schema file from %s", schema_path)

    with open(schema_path, "r") as schema_file:
        schema = json.load(schema_file)
    return schema


if __name__ == "__main__":
    sys.exit(main())
