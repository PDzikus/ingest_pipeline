"""Data ingestion endpoint.

Usage:
ingest_data --input_file <file_name> [--env <environment_name>]

Environment name should correspond to config file name in main/configs.
"""
import argparse
import logging
import os
import sys
from typing import Optional, List

import configparser

from source_processor import SourceProcessor
from main.postgres_loader import PostgresLoader

logger = logging.getLogger("Main")


def main(argv: Optional[List[str]] = None) -> None:
    args, config = initialize_context(argv)

    logger.info("Initializing source and db connection.")
    source_processor = SourceProcessor()
    event_iterator = source_processor.iterator_from_file(args.input_file)
    db_loader = PostgresLoader(
        host=config.get("Postgres", "host"),
        database=config.get("Postgres", "database"),
        user=config.get("Postgres", "user"),
        password=config.get("Postgres", "password"),
    )

    logger.info("Starting data load into PostgreSQL.")

    db_loader.load_data(data_source=event_iterator)

    logger.info("Finished data load into PostgresSQL.")

    logger.info(
        "Loaded %s valid record(s) from data source.", source_processor.valid_records
    )
    logger.info("Discarded %s invalid record(s).", source_processor.invalid_records)
    logger.info(
        "Count of records in staging table: %s",
        db_loader.count_records_in_staging_table(),
    )


def initialize_context(argv):
    configure_logging()
    args = parse_arguments(argv)
    validate_args(args)
    config = load_config(args.environment)
    return args, config


def parse_arguments(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_file",
        dest="input_file",
        required=True,
        help="Path to newline separated json data file",
    )
    parser.add_argument(
        "--env",
        dest="environment",
        required=False,
        default="local",
        help="Environment for config, by default it's set to local",
    )
    return parser.parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    if not os.path.isfile(args.input_file):
        logger.error("Input file can't be found or accessed: %s", args.input_file)
        raise FileNotFoundError


def load_config(env: str) -> configparser.ConfigParser:
    logger.info("Reading config file for environment: %s", env)
    parser = configparser.ConfigParser()
    parser.read(f"configs/config_{env.lower()}.ini")
    return parser


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
