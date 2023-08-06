import pymysql
from .data_store import cached
from .wiki_replica import query
import wikipedia_site_matrix


def fetch_babel_data(database):
    try:
        results = query(
            database,
            """
            SELECT
                DISTINCT a.actor_name AS username,
                b.babel_lang as language_used,
                b.babel_level as language_level
            FROM revision r
            JOIN change_tag ct ON r.rev_id = ct.ct_rev_id
            JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id AND ctd.ctd_name = 'contenttranslation'
            LEFT JOIN actor a ON r.rev_actor = a.actor_id
            LEFT JOIN babel b ON b.babel_user = a.actor_user
            WHERE b.babel_user IS NOT NULL
            ORDER BY a.actor_name
            """
        )
        return results
    except pymysql.OperationalError as e:
        error_msg = f"Error occurred while fetching data for database '{database}': {e}"
        print(error_msg)
    except pymysql.Error as e:
        error_msg = f"Unexpected error occurred while fetching data for database '{database}': {e}"
        print(error_msg)

    return []


@cached("translator_language_proficiency_standard_babel_new")
def format_language_proficiency(dbnames):
    allowed_languages = wikipedia_site_matrix.get_allowed_babel_languages()
    all_data = []

    for database in dbnames:
        results = fetch_babel_data(database)
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
            all_data.append({
                'username': formatted_username,
                'language_proficiency': formatted_languages,
                'wiki': database
            })

    return all_data


def generate_csv_files():
    sites = wikipedia_site_matrix.get_wikipedias()
    format_language_proficiency([site['dbname'] for site in sites])

    print("Combined CSV file saved")
