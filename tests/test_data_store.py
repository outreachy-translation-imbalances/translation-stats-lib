import data_store
from tempfile import TemporaryDirectory


def test_cached():
    with TemporaryDirectory() as tmpdir:
        data_store.configure_global_store(
            data_store.DataStore(read_from=tmpdir, write_to=tmpdir)
        )

        table = "foo"
        data = [{"x": "1", "y": "2"}, {"x": "2", "y": "3"}]

        @data_store.cached(table)
        def create():
            return data

        assert data == create()

        @data_store.cached(table)
        def read():
            return None

        assert data == read()
