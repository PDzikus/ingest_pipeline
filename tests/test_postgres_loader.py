"""Tests for postgres loader."""
import pytest

from postgres_loader import PostgresLoader
from source_processor import SourceProcessor


@pytest.fixture
def db_connection(mocker):
    return mocker.patch("psycopg2.connect")


def test_create_staging_table_should_sends_create_table_sql(db_connection):
    """Checks if any execute sent from this function creates a table."""
    loader = PostgresLoader("local", 5432, "db", "user", "pass")
    loader.create_staging_table()
    execute_params = []
    for name, args, _ in db_connection.mock_calls:
        if name.endswith("execute"):
            for arg in args:
                execute_params.append(arg)
    assert any("CREATE" in arg for arg in execute_params)
    assert any("TABLE" in arg for arg in execute_params)
    assert any(loader.table_name in arg for arg in execute_params)


def test_load_table_should_run_copy_from_with_string_iterator_producing_csv_like_structure(
    db_connection,
):
    file_path = "test_data/mixed_data.json"
    source_processor = SourceProcessor()
    file_iterator = source_processor.iterator_from_file(file_path)
    loader = PostgresLoader("local", 5432, "db", "user", "pass")
    loader.load_data(file_iterator)

    for name, args, _ in db_connection.mock_calls:
        if name.endswith("copy_from"):
            string_iterator = args[0]
    result = list(string_iterator)

    assert len(result) == 4
    assert "7|2019-01-04 05:07:26|TAFgZ@gmail.com|0603728764|2019-01-04\n" in result
    assert "6|2018-01-04 22:25:53|RNRKlh@gmail.com|5164649899|2018-01-04\n" in result
    assert "3|2018-01-08 16:27:50|fiXNIY@gmail.com|1432631330|2018-01-08\n" in result
    assert (
        "7|2019-01-14 18:49:24|PAjIeMQlnf@gmail.com|4734922788|2019-01-14\n" in result
    )
