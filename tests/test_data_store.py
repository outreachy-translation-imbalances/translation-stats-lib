from translation_stats import data_store
from tempfile import TemporaryDirectory
from unittest import mock


def test_empty_cached():
    data = [{"x": "1", "y": "2"}, {"x": "2", "y": "3"}]


    mock_empty_store = mock.Mock()
    mock_empty_store.read = mock.Mock(return_value=None)
    mock_empty_store.write = mock.Mock()

    @data_store.cached("foo", store=mock_empty_store)
    def calculate():
        return data

    assert data == calculate()
    mock_empty_store.read.assert_called_once_with("foo")
    mock_empty_store.write.assert_called_once_with("foo", data)


def test_warm_cached():
    data = [{"x": "1", "y": "2"}, {"x": "2", "y": "3"}]

    mock_warm_store = mock.Mock()
    mock_warm_store.read = mock.Mock(return_value=data)
    mock_warm_store.write = mock.Mock()

    @data_store.cached("foo", store=mock_warm_store)
    def calculate():
        raise AssertionError("Should not be called")

    assert data == calculate()
    mock_warm_store.read.assert_called_once_with("foo")
    mock_warm_store.write.assert_not_called()


def test_parameters():
    mock_store = mock.Mock()
    mock_store.read = mock.Mock(side_effect=FileNotFoundError)
    mock_store.write = mock.Mock()

    @data_store.cached("{foo}_table", store=mock_store)
    def create(foo=None):
        assert foo == "bar"
        return []

    create(foo="bar")

    mock_store.read.assert_called_once_with("bar_table")


def test_file_store():
    with TemporaryDirectory() as tmpdir:

def test_remote_store():
    def calculate_url(table):
        return dict(url="https://foo.test/" + table + ".tsv")

    def mocked_requests_get(url=None):
        class MockResponse:
            def __init__(self):
                self.status_code = 200
                self.text = "e,f\n5,6"

        assert url == "https://foo.test/bar.tsv"
        return MockResponse()

    store = data_store.RemoteStore(calculate_url)

    with mock.patch(
        "translation_stats.data_store.requests.get", side_effect=mocked_requests_get
    ):
        assert store.read() == [{"e": "5", "f": "6"}]
