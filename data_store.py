"""
Encapsulates data I/O, generalizable to any type of storage backend.

Currently focused on *where* to store the data and not its formatting.
"""

import csv
from functools import wraps
import os.path
import requests
from typing import Callable, List


def _filesystem_path(root, table):
    return os.path.join(root, table) + ".csv"


def _read_csv(path) -> List[dict]:
    with open(path) as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def _write_csv(path, data: List[dict]):
    with open(path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)


class DataStore:
    def __init__(
        self,
        local_sources: List[str] = [],
        remote_sources: List[Callable[[str], dict]] = [],
        output_path=".",
    ):
        # List of local paths containing data.  Will implicitly include the
        # output_path as the first place to check.
        self.local_sources = local_sources
        self.local_sources.insert(0, output_path)
        # List of functions which calculate a request from a given table.
        self.remote_sources = remote_sources
        self.output_path = output_path

    def read(self, table) -> List[dict]:
        for source in self.local_sources:
            try:
                return _read_csv(_filesystem_path(source, table))
            except FileNotFoundError:
                pass

        for source in self.remote_sources:
            result = requests.get(**source(table))
            if result.status_code != 200:
                continue

            # Mirror raw data to the local directory.
            path = _filesystem_path(self.output_path, table)
            with open(path, "w") as f:
                f.write(result.text)
            return _read_csv(path)

        raise FileNotFoundError("No source found for " + table)

    def write(self, table, data) -> None:
        # TODO: flexible formatting
        _write_csv(_filesystem_path(self.output_path, table), data)


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
