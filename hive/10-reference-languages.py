#!/usr/bin/env python3
"""
Import the current list of all wikipedia language editions.
"""

import os
import requests
import wmfdata.spark
import wmfdata.utils


database = os.getenv('DATABASE')


def get_wikipedia_languages():
    matrix = requests.get(
        url="https://meta.wikimedia.org/w/api.php",
        params={
            "action": "sitematrix",
            "format": "json",
            "formatversion": 2,
            "smlangprop": "code|site",
            "smsiteprop": "code",
            "smtype": "language",
        },
    ).json()["sitematrix"]
    del matrix["count"]
    languages = []
    for group in matrix.values():
        for site in group["site"]:
            if site["code"] == "wiki" and "closed" not in site:
                languages.append(group["code"])
    return languages

spark = wmfdata.spark.create_session()
result = [[lang] for lang in get_wikipedia_languages()]
spark.createDataFrame(result, "lang: string")\
    .write.saveAsTable(f"{database}.wikipedia_languages")

# 326 rows
