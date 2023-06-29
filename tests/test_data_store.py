from translation_stats import data_store
from tempfile import TemporaryDirectory
from unittest import mock


def test_cached():
    with TemporaryDirectory() as tmpdir:
        data_store.configure_global_store(data_store.DataStore(output_path=tmpdir))

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


def test_remote():
    with TemporaryDirectory() as tmpdir:

        def calculate_url(table):
            return dict(url="https://foo.test/" + table)

        store = data_store.DataStore(remote_sources=[calculate_url], output_path=tmpdir)
        # FIXME: not ideal to override a global from tests
        data_store.configure_global_store(store)

        def mocked_requests_get(url=None):
            class MockResponse:
                def __init__(self):
                    self.status_code = 200
                    self.text = "e,f\n5,6"

            assert url == "https://foo.test/bar"
            return MockResponse()

        @data_store.cached("bar")
        def read():
            return None

        with mock.patch(
            "translation_stats.data_store.requests.get", side_effect=mocked_requests_get
        ):
            assert read() == [{"e": "5", "f": "6"}]
