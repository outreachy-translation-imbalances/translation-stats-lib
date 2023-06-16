"""
Encapsulates data I/O, generalizable to any type of storage backend.

Currently focused on *where* to store the data and not its formatting.
"""

import csv
from functools import wraps
import os.path


def _filesystem_path(root, table):
    return os.path.join(root, table) + ".csv"


def _read_csv(path) -> list[dict]:
    print("reading path " + path)
    with open(path) as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def _write_csv(path, data: list[dict]):
    print("writing to path " + path)
    with open(path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


class DataStore:
    def __init__(self, read_from=".", write_to=".") -> None:
        # Data root directory
        # TODO: Support a fallback list and URL
        self.read_from = read_from
        # Data root directory
        self.write_to = write_to

    def read(self, table) -> list[dict]:
        return _read_csv(_filesystem_path(self.read_from, table))

    def write(self, table, data) -> None:
        # TODO: flexible formatting
        _write_csv(_filesystem_path(self.write_to, table), data)


def cached(table):
    """
    Decorator memoizes results to the filesystem.

    Usage:
        @cached("table_name")
        def calculate_expensive(): ...
    """

    def decorated(func):
        @wraps(func)
        def wrapped_calculation():
            try:
                return _global_store.read(table)

            except FileNotFoundError:
                data = func()

                _global_store.write(table, data)
                return data

        return wrapped_calculation

    return decorated


_global_store = DataStore()


def configure_global_store(new_store: DataStore):
    global _global_store
    _global_store = new_store
