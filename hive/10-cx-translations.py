#!/usr/bin/env python3
"""
Extract Content Translation metadata from the database.
"""

import os
import wmfdata.mariadb
import wmfdata.spark
import wmfdata.utils


database = os.getenv('DATABASE')


cx_pairs = wmfdata.mariadb.run(
    """
    select
        translation_id,
        translation_target_revision_id as target_revision_id,
        translation_started_by as global_user_id,
        translation_source_language as source_language,
        translation_target_language as target_language,
        translation_cx_version as cx_version,
        from_json(translation_progress, 'map<string, double>') as translation_progress
    from cx_translations
    where
        (translation_status = 'published' or translation_target_url is not null)
    """,
    "wikishared"
)

spark = wmfdata.spark.create_session()
spark.createDataFrame(cx_pairs)\
    .write.saveAsTable(f"{database}.cx_translations")

# 1,657,167 rows
