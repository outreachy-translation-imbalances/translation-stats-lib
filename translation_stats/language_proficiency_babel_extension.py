"""
Extract Babel extension language proficiencies directly from the database tables
generated by the Babel parser function.
"""
from .data_store import cached
from .wiki_replica import query
from .wikipedia_site_matrix import get_languages


def fetch_babel_data(database):
    # TODO: Reuse the results of content_translation_revisions.
    return query(
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


@cached("language_proficiency_babel_extension")
def format_language_proficiency(dbnames):
    allowed_languages = get_languages()
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

        for username, language in merged_rows.items():
            formatted_username = f"{username}"
            formatted_languages = ", ".join([f"{lang}-{prof}" for lang, prof in language])
            all_data.append({
                'username': formatted_username,
                'language_proficiency': formatted_languages,
                'wiki': database
            })

    return all_data