"""
Encapsulates data I/O, generalizable to any type of storage backend.

Currently focused on *where* to store the data and not its formatting.

Defaults are to read and write to the local directory.

Usage example which read and writes to /tmp, and falls back to pulling data
from a public notebook.  Evaluation will have the side effect of saving the
remote data to the local directory.

>>> def notebook_url(table):
...     return dict(url="https://public-paws.wmcloud.org/User:Adamw/Translation%20Imbalances/" + table + ".csv")
>>> store = data_store.DataStore(remote_sources=[notebook_url], output_path="/tmp")
>>> configure_global_store(store)

>>> @cached("content_translation_stats")
... def demo():
...     return None

>>> demo()[0]
{'sourceLanguage': 'ady', 'targetLanguage': 'tr', 'status': 'draft', 'count': '1', 'translators': '1'}
"""

import csv
from functools import wraps
import os
import os.path
import requests
from typing import Callable, List


def _filesystem_path(root, table):
    return os.path.abspath(os.path.join(root, table) + ".csv")


def _read_csv(path) -> List[dict]:
    with open(path) as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def _write_csv(path, data: List[dict]):
    if data:
        with open(path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        print("csv generated successfully at:", path)
    else:
        print("No data to write to the CSV file.")


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
        path = _filesystem_path(self.output_path, table)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _write_csv(path, data)


def cached(table):
    """
    Decorator memoizes results to the filesystem.

    Without parameters:
        @cached("table_name")
        def calculate_expensive(): ...

    With parameters:
        @cached("{wiki}_table_name")
        def calculate_expensive(*, wiki): ...
    """

    def decorated(func):
        @wraps(func)
        def wrapped_calculation(*args, **kwargs):
            filename = table.format(*args, **kwargs)

            try:
                return _global_store.read(filename)

            except FileNotFoundError:
                data = func(*args, **kwargs)

                _global_store.write(filename, data)
                return data

        return wrapped_calculation

    return decorated


_global_store = DataStore()


def configure_global_store(new_store: DataStore):
    global _global_store
    _global_store = new_store
