"""PostgreSQL loader."""
import io
import logging
from typing import Iterator
import psycopg2

from models.event import Event


class PostgresLoader:
    """Class responsible for PostgreSQL interaction."""

    # TODO: initialize based on configuration. Pass shouldn't be in code btw.
    def __init__(self, host: str, database: str, user: str, password: str):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.table_name = "staging"
        self.connection = psycopg2.connect(
            host=host, database=database, user=user, password=password
        )
        self.connection.autocommit = True

    def create_staging_table(self) -> None:
        """Creates staging table."""
        self.logger.info("Creating table %s.", self.table_name)
        with self.connection.cursor() as cursor:
            cursor.execute(Event.staging_table_creation_sql(self.table_name))

    def count_records_in_staging_table(self) -> int:
        """Retrieves number of records in staging table."""
        with self.connection.cursor() as cursor:
            cursor.execute(f"SELECT COUNT(*) FROM {self.table_name};")
            response = cursor.fetchone()
        return response[0]

    def load_data(self, data_source: Iterator[Event]) -> None:
        """Loads data from Iterator to database."""
        self.create_staging_table()
        self.logger.info("Starting data load")
        with self.connection.cursor() as cursor:
            data_string_iterator = StringIteratorIO(
                (event.to_csv_row() for event in data_source)
            )
            cursor.copy_from(data_string_iterator, self.table_name, sep="|")


class StringIteratorIO(io.TextIOBase):
    """This class allows to read from a provided iterator as a file-like object.

    Implementation based on
    https://stackoverflow.com/questions/12593576/adapt-an-iterator-to-behave-like-a-file-like-object-in-python
    """

    def __init__(self, iterator: Iterator[str]):
        self._iter = iterator
        self._left = ""

    def readable(self) -> bool:
        """Checks if StringIterator is readable."""
        return True

    def _read1(self, n=None):
        while not self._left:
            try:
                self._left = next(self._iter)
            except StopIteration:
                break
        ret = self._left[:n]
        self._left = self._left[len(ret) :]
        return ret

    def read(self, n=None):
        """Reads at max n characters from Iterator, if n=None, reads until end of the Iterator."""
        lines = []
        if n is None or n < 0:
            while True:
                line = self._read1()
                if not line:
                    break
                lines.append(line)
        else:
            while n > 0:
                line = self._read1(n)
                if not line:
                    break
                n -= len(line)
                lines.append(line)
        return "".join(lines)

    def readline(self):
        """Reads from the iterator until end of line has been found."""
        line = []
        while True:
            i = self._left.find("\n")
            if i == -1:
                line.append(self._left)
                try:
                    self._left = next(self._iter)
                except StopIteration:
                    self._left = ""
                    break
            else:
                line.append(self._left[: i + 1])
                self._left = self._left[i + 1 :]
                break
        return "".join(line)
