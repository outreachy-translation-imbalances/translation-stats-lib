"""
Find all users who have contributed a translation using the Content Translation extension.
"""
from .data_store import cached
from .wiki_replica import query
from .wikipedia_site_matrix import get_wikipedias

@cached("translator_usernames/{wiki}_usernames")
def fetch_usernames(*, wiki):
    results = query(
        wiki,
        """
        SELECT
            DISTINCT r.rev_actor AS userid,
            a.actor_name AS username
        FROM revision r
        JOIN change_tag ct ON r.rev_id = ct.ct_rev_id
        JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id
            AND ctd.ctd_name = 'contenttranslation'
        LEFT JOIN actor a ON r.rev_actor = a.actor_id
        """
    )

    print(f"Number of rows in results for {wiki}: {len(results)}")

    return results


def get_userpage_titles(wiki):
    return ["User:" + row['username'] for row in fetch_usernames(wiki=wiki)]


def generate_csv_files():
    for database_name in get_wikipedias():
        fetch_usernames(wiki=database_name)
