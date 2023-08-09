"""
Extract revision metadata for changes tagged with "contenttranslation".
"""
from .data_store import cached
from .wiki_replica import query


@cached("content_translation_revisions/{dbname}_revisions")
def process_database(*, dbname):
    results = query(
        dbname,
        """
        SELECT
            r.rev_id as revision_id,
            a.actor_name AS username,
            r.rev_timestamp as revision_timestamp,
            c.comment_text as comment
        FROM revision r
        JOIN change_tag ct ON r.rev_id = ct.ct_rev_id
        JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id
            AND ctd.ctd_name = 'contenttranslation'
        LEFT JOIN actor a ON r.rev_actor = a.actor_id
        LEFT JOIN comment c ON r.rev_comment_id = c.comment_id
        """
    )

    data = []
    for row in results:
        comment = row['comment']

        source_wiki = ''
        if '[[:' in comment:
            start_index = comment.find('[[:') + 3
            end_index = comment.find(':', start_index)
            if end_index != -1:
                source_wiki = comment[start_index:end_index] + "wiki"

        row['source_wiki'] = source_wiki
        row['target_wiki'] = dbname
        data.append(row)

    return data
