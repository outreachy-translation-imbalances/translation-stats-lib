"""
Extract language proficiencies from the metawiki global user database.
"""
from .data_store import cached
from .wiki_replica import query
from .translators import fetch_usernames
from . import wikipedia_site_matrix


def fetch_babel_data(database):
    return query(
        database,
        """
        SELECT
            DISTINCT a.actor_name AS username,
            b.babel_lang as language_used,
            b.babel_level as language_level
        FROM actor a
        LEFT JOIN babel b ON b.babel_user = a.actor_user
        WHERE b.babel_user is not null
        ORDER BY a.actor_name
        """
    )


def format_language_proficiency(results):
    allowed_languages = wikipedia_site_matrix.get_languages()
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


def match_usernames(babel_data):
    # Load the usernames and languages from babel_data
    metawiki_usernames = set()
    metawiki_usernames_languages = {}
    for row in babel_data:
        username = row['username']
        language = row['language_proficiency']
        metawiki_usernames.add(username)
        metawiki_usernames_languages[username] = language

    sites = wikipedia_site_matrix.get_wikipedias()
    matched_rows = []
    for site in sites:
        users = fetch_usernames(wiki=site['dbname'])
        for user in users:
            username = user['username']
            if username in metawiki_usernames:
                language = metawiki_usernames_languages[username]
                matched_rows.append({
                    'username': username,
                    'language_proficiency': language,
                    'wiki': "metawiki"
                })

    return matched_rows


@cached("language_proficiency_global_babel")
def get_metawiki_proficiency():
    results = fetch_babel_data('metawiki')
    formatted_results = format_language_proficiency(results)
    return match_usernames(formatted_results)
