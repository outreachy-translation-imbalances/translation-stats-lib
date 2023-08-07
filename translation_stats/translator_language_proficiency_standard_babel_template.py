import pymysql
from .wiki_replica import query
from . import wikipedia_site_matrix
from .data_store import cached


def fetch_babel_data(database):
    try:
        return query(
            database,
            """
            SELECT
                DISTINCT a.actor_name AS username,
                b.babel_lang as language_used,
                b.babel_level as language_level
            FROM revision r
            JOIN change_tag ct ON r.rev_id = ct.ct_rev_id
            JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id
                AND ctd.ctd_name = 'contenttranslation'
            LEFT JOIN actor a ON r.rev_actor = a.actor_id
            LEFT JOIN babel b ON b.babel_user = a.actor_user
            WHERE b.babel_user IS NOT NULL
            ORDER BY a.actor_name
            """
        )
    except pymysql.OperationalError as e:
        error_msg = f"Error occurred while fetching data for database '{database}': {e}"
        print(error_msg)
    except pymysql.Error as e:
        error_msg = f"Unexpected error occurred while fetching data for database '{database}': {e}"
        print(error_msg)

    return []

def format_language_proficiency(results):
    allowed_languages = wikipedia_site_matrix.get_allowed_babel_languages()
    merged_rows = {}
    for row in results:
        username = row['username']
        language = row['language_used']
        proficiency = row['language_level']
        if language in allowed_languages:
            if username not in merged_rows:
                merged_rows[username] = [(language, proficiency)]
            else:
                merged_rows[username].append((language, proficiency))

    output_rows = []
    for username, language in merged_rows.items():
        formatted_username = f"{username}"
        formatted_languages = ", ".join([f"{lang}-{prof}" for lang, prof in language])
        output_rows.append({
            'username': formatted_username,
            'language_proficiency': formatted_languages
        })

    return output_rows


@cached("translator_language_proficiency_standard_babel/{dbname}_translator_language_proficiency_standard_babel")
def get_language_proficiency_standard_babel(*, dbname):
    results = fetch_babel_data(dbname)
    return format_language_proficiency(results)


def generate_csv_files():
    dbnames = [site['dbname'] for site in wikipedia_site_matrix.get_wikipedias()]
    for database in dbnames:
        get_language_proficiency_standard_babel(dbname=database)
