import requests
import csv

def fetch_response():
    url = "https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise ConnectionError("Failed to retrieve the Wikipedia language data.")

def process_response(data):
    language_codes = []
    dbnames = []
    urls = []
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

            # Append the language codes, dbnames, and urls to their respective lists
            language_codes.append(group_language_codes)
            dbnames.append(group_dbname)
            urls.append(group_url)

    save_language_data(language_data)
    print("Language data saved successfully.")


def save_language_data(language_data):
    with open("/home/paws/translation-stats-data/language_data.csv", "w", encoding="utf-8", newline="") as file:
        fieldnames = ["Language Code", "DB Name", "URL"]
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(language_data)

def generate_csv_files():
    response = fetch_response()
    process_response(response)