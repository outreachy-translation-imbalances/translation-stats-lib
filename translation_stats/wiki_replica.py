"""
Connect to a Wikimedia database replica on PAWS
"""
import pymysql
from pymysql.constants import FIELD_TYPE


def _decode_binary(value):
    """Decode UTF-8 bytes to a string."""
    if isinstance(value, bytes):
        return value.decode()
    else:
        return value


def _make_connection(wiki, replica_type="analytics"):
    """Connects to a host and database of the same name.

    `replica_type` can be either "analytics" (default), or "web"."""
    assert replica_type == "web" or replica_type == "analytics"

    conv = pymysql.converters.conversions
    conv[FIELD_TYPE.BLOB] = _decode_binary
    conv[FIELD_TYPE.TINY_BLOB] = _decode_binary
    conv[FIELD_TYPE.MEDIUM_BLOB] = _decode_binary
    conv[FIELD_TYPE.LONG_BLOB] = _decode_binary
    conv[FIELD_TYPE.STRING] = _decode_binary
    conv[FIELD_TYPE.VAR_STRING] = _decode_binary

    return pymysql.connect(
        host=f"{wiki}.{replica_type}.db.svc.wikimedia.cloud",
        read_default_file="~/.my.cnf",
        database=f"{wiki}_p",
        charset="utf8",
        conv=conv,
        cursorclass=pymysql.cursors.DictCursor,
    )


def query(wiki, query):
    """
    Execute a SQL query against the connection, and return **all** the results,
    as a list of associative dictionaries.
    """
    conn = _make_connection(wiki)
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
    conn.close()
    return data
