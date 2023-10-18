#!/usr/bin/env python
"""
Run through all data source calculations, populating outputs.

This script is hardcoded and is meant for development and experimentation--edit
the configuration directly for now.
"""
import os.path

from translation_stats import data_store
from translation_stats import language_proficiency_babel_template
from translation_stats import language_proficiency_babel_extension
from translation_stats import language_proficiency_user_template
from translation_stats import language_proficiency_global_babel
from translation_stats import content_translation_revisions
from translation_stats.wikipedia_site_matrix import get_wikipedias


data_path = os.path.expanduser("~/translation-stats-data")


if __name__ == "__main__":
    default_store = data_store.DataStore(output_path=data_path)
    data_store.configure_global_store(default_store)

    sites = get_wikipedias()
    dbnames = [site['dbname'] for site in sites]
    language_proficiency_babel_template.process_data_and_create_csv(sites)
    language_proficiency_babel_extension.format_language_proficiency(dbnames)
    language_proficiency_user_template.process_data_and_create_csv(sites)
    language_proficiency_global_babel.get_metawiki_proficiency()
    for site in sites:
        content_translation_revisions.process_database(dbname=site['dbname'])
