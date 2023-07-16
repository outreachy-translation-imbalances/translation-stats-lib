import pymysql
import csv
import socket
from .data_store import cached


def make_connection(wiki, replica_type="analytics"):
    """Connects to a host and database of the same name.
    
    `replica_type` can be either "analytics" (default) or "web"."""
    assert replica_type in ("analytics", "web")

    try:
        connection = pymysql.connect(
            host=f"{wiki}.{replica_type}.db.svc.wikimedia.cloud",
            read_default_file="~/.my.cnf",
            database=f"{wiki}_p",
            charset='utf8'
        )
        return connection
    except socket.gaierror as e:
        error_msg = f"Hostname resolution failed: {e}"
        print(error_msg)
        raise pymysql.Error(error_msg)
    except pymysql.Error as e:
        error_msg = f"Error occurred while connecting to the database: {e}"
        print(error_msg)
        raise pymysql.Error(error_msg)


def query(conn, query):
    """Execute a SQL query against the connection, and return **all** the results."""
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        return data

def fetch_babel_data(database):
    try:
        conn = make_connection(database)
        results = query(
            conn,
            """
            SELECT Distinct a.actor_name AS username, b.babel_lang as language_used, b.babel_level as language_level 
            FROM revision r JOIN change_tag ct ON r.rev_id = ct.ct_rev_id 
            JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id AND ctd.ctd_name = 'contenttranslation' 
            LEFT JOIN actor a ON r.rev_actor = a.actor_id 
            LEFT JOIN babel b ON b.babel_user = a.actor_user 
            WHERE b.babel_user IS NOT NULL 
            ORDER BY a.actor_name ;
            """
        )
        conn.close()
        return results
    except pymysql.OperationalError as e:
        error_msg = f"Error occurred while fetching data for database '{database}': {e}"
        print(error_msg)
    except pymysql.Error as e:
        error_msg = f"Unexpected error occurred while fetching data for database '{database}': {e}"
        print(error_msg)

    return []


@cached("/home/paws/translation-stats-data/translator_language_proficiency_standard_babel_new")
def format_language_proficiency(dbnames, allowed_languages):
    all_data = []

    for database in dbnames:
        results = fetch_babel_data(database)
        merged_rows = {}
        for result in results:
            username = result[0].decode('utf-8')
            language = result[1].decode('utf-8')
            proficiency = result[2].decode('utf-8')
            if language in allowed_languages:
                if username not in merged_rows:
                    merged_rows[username] = [(language, proficiency)]
                else:
                    merged_rows[username].append((language, proficiency))

        output_rows = []
        for username, language in merged_rows.items():
            formatted_username = f"{username}"
            formatted_languages = ", ".join([f"{lang}-{prof}" for lang, prof in language])
            output_rows.append({'Username': formatted_username, 'Language-Proficiency': formatted_languages})
            all_data.append({'Username': formatted_username, 'Language-Proficiency': formatted_languages, 'Wikipedia Version': database})

    return all_data


def generate_csv_files():
    # Read the database names from the language_data.csv file
    dbnames = []
    allowed_languages = []
    with open('/home/paws/translation-stats-data/language_data.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            dbnames.append(row['DB Name'])
            allowed_languages.append(row['Language Code'])

    all_data = format_language_proficiency(dbnames, allowed_languages)

    print(f"Combined CSV file saved")