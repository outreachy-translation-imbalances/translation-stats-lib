"""
Build a list of all active Wikipedias, giving their language code and the base
URL for constructing API requests.

>>> get_wikipedias()
[
	{
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
            "smsiteprop": "code|url",
            "smtype": "language",
        },
    ).json()["sitematrix"]

    del matrix["count"]

    sites = []
    for group in matrix.values():
        if "site" in group:
            for site in group["site"]:
                if site["code"] == "wiki" and "closed" not in site:
                    sites.append({"language": group["code"], "url": site["url"]})

    return sites
