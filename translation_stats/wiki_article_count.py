"""
Provide a map from language name to Wikipedia article count (only including the
so-called "content" pages, so excluding talk pages) and active editor count.

>>> get_wiki_stats()
[
	{
        "language": "zea",
        "articles": 5738,
        "active_users": 26
    },
	...
]
"""

from .data_store import cached
import requests
from .wikipedia_site_matrix import get_wikipedias


@cached("wikipedia_statistics")
def get_wiki_stats():
    sites = get_wikipedias()
    result = []
    for site in sites:
        api_url = site["url"] + "/w/api.php"
        stats = requests.get(
            url=api_url,
            params={
                "action": "query",
                "format": "json",
                "formatversion": 2,
                "meta": "siteinfo",
                "siprop": "statistics",
            },
        ).json()["query"]["statistics"]
        result.append(
            {
                "language": site["language"],
                "articles": stats["articles"],
                "active_users": stats["activeusers"],
            }
        )
    return result
