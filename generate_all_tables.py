#!/usr/bin/env python
"""
Run through all data source calculations, populating outputs.

This script is hardcoded and is meant for development and experimentation--edit
the configuration directly for now.
"""
import os.path

from translation_stats import data_store
from translation_stats import compiled_translator_language_proficiency_misc_babel
from translation_stats import compiled_translator_language_proficiency_standard_babel
from translation_stats import compiled_translator_language_proficiency_user_template
from translation_stats import metawiki_translator_language_proficiency_standard_babel
from translation_stats import revision_table


data_path = os.path.expanduser("~/translation-stats-data")


if __name__ == "__main__":
    default_store = data_store.DataStore(output_path=data_path)
    data_store.configure_global_store(default_store)

    compiled_translator_language_proficiency_misc_babel.generate_csv_files()
    compiled_translator_language_proficiency_standard_babel.generate_csv_files()
    compiled_translator_language_proficiency_user_template.generate_csv_files()
    metawiki_translator_language_proficiency_standard_babel.generate_csv_files()
    revision_table.generate_csv_files()
