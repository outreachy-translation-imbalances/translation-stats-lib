"""
Connect to a Wikimedia database replica on PAWS
"""
import pymysql
import socket


def _make_connection(wiki, replica_type="analytics"):
    """Connects to a host and database of the same name.
    
    `replica_type` can be either "analytics" (default), or "web"."""
    assert replica_type == "web" or replica_type == "analytics"
    try:
        return pymysql.connect(
            host=f"{wiki}.{replica_type}.db.svc.wikimedia.cloud",
            read_default_file="~/.my.cnf",
            database=f"{wiki}_p",
            charset='utf8'
        )
    except socket.gaierror as e:
        error_msg = f"Hostname resolution failed: {e}"
        print(error_msg)
        raise pymysql.Error(error_msg)
    except pymysql.Error as e:
        error_msg = f"Error occurred while connecting to the database: {e}"
        print(error_msg)
        raise pymysql.Error(error_msg)


def query(wiki, query):
    """Execute a SQL query against the connection, and return **all** the results."""
    conn = _make_connection(wiki)
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
    conn.close()
    return data
