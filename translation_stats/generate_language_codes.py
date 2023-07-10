import requests
import csv
from .data_store import cached


@cached("wikipedia_sites_new")
def retrieve_language_data():
    url = "https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json"
    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        language_data = []

        sitematrix = data.get("sitematrix", {})
        for group_key, group_value in sitematrix.items():
            if isinstance(group_value, dict) and "site" in group_value:
                group_language_codes = group_value.get("code")
                sites = group_value["site"]
                for site in sites:
                    if site.get("code") == "wiki":
                        group_dbname = site.get("dbname")
                        group_url = site.get("url")
                        language_data.append({"Language Code": group_language_codes, "DB Name": group_dbname, "URL": group_url})

        return language_data

    else:
        raise ConnectionError("Failed to retrieve the Wikipedia language data.")
