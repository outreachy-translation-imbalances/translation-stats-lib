"""
Build a list of all active Wikipedias, giving their language code and the base
URL for constructing API requests.

>>> get_wikipedias()
[
	{
        "dbname": "yowiki",
        "language": "yo",
        "url": "https://yo.wikipedia.org"
	},
	...
]
"""
from .data_store import cached
import requests


# Adapted from https://github.com/Abhishek02bhardwaj/Translators_Data
@cached("wikipedia_sites")
def get_wikipedias():
    matrix = requests.get(
        url="https://en.wikipedia.org/w/api.php",
        params={
            "action": "sitematrix",
            "format": "json",
            "formatversion": 2,
            "smlangprop": "code|site",
            "smsiteprop": "code|dbname|url",
            "smtype": "language",
        },
    ).json()["sitematrix"]

    del matrix["count"]

    sites = []
    for group in matrix.values():
        if "site" in group:
            for site in group["site"]:
                if site["code"] == "wiki" and "closed" not in site:
                    sites.append(
                        {
                            "dbname": site["dbname"],
                            "language": group["code"],
                            "url": site["url"],
                        }
                    )

    return sites


def get_languages():
    return [site["language"] for site in get_wikipedias()]


@cached("allowed_language_proficiencies")
def get_allowed_babel_language_proficiencies():
    """Build a list of all possible Babel proficiency levels for known Wikipedia langauges."""
    allowed_languages = []
    for code in get_languages():
        allowed_languages.append(code)
        allowed_languages.append(f"{code}-N")
        for i in range(6):
            version = f"{code}-{i}"
            allowed_languages.append(version)

    return allowed_languages
