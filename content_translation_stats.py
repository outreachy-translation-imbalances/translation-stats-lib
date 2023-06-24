"""
Wrap the aggregate statistics returned by the contenttranslationstats API.

>>> get_published()
[
	{
		'sourceLanguage': 'ab',
		'targetLanguage': 'id',
		'status': 'published',
		'count': '1',
		'translators': '1'
	},
	{
		'sourceLanguage': 'ace',
		'targetLanguage': 'bn',
		'status': 'published',
		'count': '1',
		'translators': '1'
	},
	...
]
"""
from collections import defaultdict
from data_store import cached
import requests


@cached("content_translation_stats")
def get_stats():
    """
    Return statistics in the raw upstream form, which includes summary detail
    for every known translation language pair, and both draft and published
    translations.
    """
    return (
        requests.Session()
        .get(
            url="https://en.wikipedia.org/w/api.php",
            params={
                "action": "query",
                "list": "contenttranslationstats",
                "format": "json",
            },
        )
        .json()["query"]["contenttranslationstats"]["pages"]
    )


def get_published():
    """Filter to just the published translations"""
    return [pair for pair in get_stats() if pair["status"] == "published"]


def get_translated_out():
    """
    Aggregate the number of translated articles where a given language was
    the source.
    """
    translated_out = defaultdict(int)
    for pair in get_published():
        translated_out[pair["sourceLanguage"]] += int(pair["count"])

    return translated_out
