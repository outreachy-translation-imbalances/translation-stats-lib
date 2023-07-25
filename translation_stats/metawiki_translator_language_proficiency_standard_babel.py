import pymysql
import csv
import socket
import os
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
            """ SELECT Distinct a.actor_name AS username, b.babel_lang as language_used, b.babel_level as language_level 
            From actor a LEFT JOIN babel b ON b.babel_user = a.actor_user 
            where b.babel_user is not null 
            order by a.actor_name ;"""
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


def format_language_proficiency(results, allowed_languages):
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

    return output_rows


@cached("/home/paws/translation-stats-data/metawiki_translator_language_proficiency_standard_babel")
def match_usernames(babel_data, usernames_folder):
    # Load the usernames and languages from babel_data
    metawiki_usernames = set()
    metawiki_usernames_languages = {}
    for row in babel_data:
        username = row['Username']
        language = row['Language-Proficiency']
        metawiki_usernames.add(username)
        metawiki_usernames_languages[username] = language

    # Iterate through the CSV files in the translator_usernames folder
    matched_rows = []
    for filename in os.listdir(usernames_folder):
        if filename.endswith('.csv'):
            filepath = os.path.join(usernames_folder, filename)
            with open(filepath, 'r', encoding='utf-8') as username_file:
                username_reader = csv.DictReader(username_file)
                for row in username_reader:
                    username = row['Username']
                    if username in metawiki_usernames:
                        language = metawiki_usernames_languages[username]
                        matched_rows.append({'Username': username, 'Language-Proficiency': language, 'Wikipedia Version': "metawiki"})
                        
    return matched_rows


def generate_csv_files():
    # Read the database names from the language_data.csv file
    dbnames = ['metawiki']
    allowed_languages = []
    with open('/home/paws/translation-stats-data/language_data.csv', 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            allowed_languages.append(row['Language Code'])

    babel_data = []
    for database in dbnames:
        results = fetch_babel_data(database)
        formatted_results = format_language_proficiency(results, allowed_languages)
        babel_data.extend(formatted_results)

    # Specify the paths
    usernames_folder = '/home/paws/translation-stats-data/translator_usernames'
    # Call the function to match and copy the usernames and languages
    match_usernames(babel_data, usernames_folder)