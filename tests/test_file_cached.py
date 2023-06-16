from file_cached import cached
import os
from tempfile import NamedTemporaryFile


def test_cached():
    f = NamedTemporaryFile()
    os.remove(f.name)

    data = [{"x": "1", "y": "2"}, {"x": "2", "y": "3"}]

    @cached(f.name)
    def create():
        return data

    assert data == create()
    assert os.path.exists(f.name)

    @cached(f.name)
    def read():
        return None

    assert data == read()
