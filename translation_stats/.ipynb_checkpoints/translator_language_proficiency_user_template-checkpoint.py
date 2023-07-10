import requests
import csv
import pymysql
import re
import json


def fetch_language_data():
    url = "https://meta.wikimedia.org/w/api.php?action=sitematrix&format=json"
    response = requests.get(url)
    language_codes = []
    dbnames = []
    urls = []

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

                # Append the language codes, dbnames, and urls to their respective lists
                language_codes.append(group_language_codes)
                dbnames.append(group_dbname)
                urls.append(group_url)

        with open("/home/paws/translation-stats-data/language_data.csv", "w", encoding="utf-8", newline="") as file:
            fieldnames = ["Language Code", "DB Name", "URL"]
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(language_data)

        print("Language data saved successfully.")
    else:
        raise ConnectionError("Failed to retrieve the Wikipedia language data.")

    return language_codes, dbnames, urls


def make_connection(wiki, replica_type="analytics"):
    """Connects to a host and database of the same name.
    
    `replica_type` can be either "analytics" (default), or "web"."""
    assert replica_type == "web" or replica_type == "analytics"
    return pymysql.connect(
        host=f"{wiki}.{replica_type}.db.svc.wikimedia.cloud",
        read_default_file="~/.my.cnf",
        database=f"{wiki}_p",
        charset='utf8'
    )


def query(conn, query):
    """Execute a SQL query against the connection, and return **all** the results."""
    with conn.cursor() as cur:
        cur.execute(query)
        data = cur.fetchall()
        return data


def fetch_usernames(dbnames):
    for database_name in dbnames:
        try:
            conn = make_connection(database_name)
            results = query(
                conn,
                "SELECT DISTINCT r.rev_actor AS userid, a.actor_name AS username FROM revision r JOIN change_tag ct ON r.rev_id = ct.ct_rev_id JOIN change_tag_def ctd ON ct.ct_tag_id = ctd.ctd_id AND ctd.ctd_name = 'contenttranslation' LEFT JOIN actor a ON r.rev_actor = a.actor_id"
            )

            print(f"Number of rows in results for {database_name}: {len(results)}")

            user_ids = []
            usernames = []

            for result in results:
                user_ids.append(result[0])
                usernames.append(result[1].decode("utf-8"))

            with open(f'/home/paws/translation-stats-data/translator_usernames/{database_name}_usernames.csv', 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['User ID', 'Username'])

                if len(user_ids) == 0:
                    print("No results found.")
                else:
                    for i in range(len(user_ids)):
                        writer.writerow([user_ids[i], usernames[i]])

            print(f"CSV file for {database_name} generated successfully.")

            conn.close()
        except pymysql.Error as e:
            print(f"Error occurred while processing {database_name}: {e}")


def get_user_titles_with_babel_from_csv(csv_file):
    titles = []
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row if present
        for row in reader:
            titles.append("User:" + row[1])  # Assuming usernames are in the second column
    return titles


def extract_language_codes(template_text):
    code_pattern = r"(?<=\|)([^\|\[\]]+)(?=\|)"
    languages = re.findall(code_pattern, template_text)
    first_element = re.search(r"^[^\|]+", template_text)
    last_element = re.search(r"[^\|]+$", template_text)
    if first_element:
        first_element = first_element.group()
        languages.append(re.sub(r"^\['", '', first_element))
    if last_element:
        last_element = last_element.group()
        last_element = re.sub(r"'\]$", '', last_element)
        if last_element:
            languages.append(last_element)
    return languages


def fetch_content(url, params):
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.text
    return None


def parse_babel_templates(content, allowed_languages, dbname):
    if dbname == "frwiki":
        babel_templates = re.findall(r"\{\{Utilisateur(?![^:}]*?:)(?:[^}]*?)[^\w}](.*?)\}\}", content, re.IGNORECASE)
    else:
        babel_templates = re.findall(r"\{\{User(?![^:}]*?:)(?:[^}]*?)[^\w}](.*?)\}\}", content, re.IGNORECASE)
    language_claims = []
    if babel_templates:
        for babel_template in babel_templates:
            user_text = str(babel_template)
            user_languages = extract_language_codes(user_text)
            valid_user_languages = set()
            for lang in user_languages:
                lang = lang.strip()
                lang = lang.rstrip('\n')
                if lang in allowed_languages:
                    valid_user_languages.add(lang)
            language_claims.extend(valid_user_languages)
    return language_claims

def find_babel_languages(title, allowed_languages, url, dbname):
    modified_url = url + "/w/index.php"
    params = {
        "title": title,
        "action": "raw"
    }
    content = fetch_content(modified_url, params)
    if content:
        language_claims = parse_babel_templates(content, allowed_languages, dbname)
        return title, language_claims
    return title, []


def create_user_language_csv(csv_file, output_file, allowed_languages, url, dbname):
    titles = get_user_titles_with_babel_from_csv(csv_file)
    results = [find_babel_languages(title, allowed_languages, url, dbname) for title in titles]
    filtered_results = [(title, languages) for title, languages in results if languages]

    with open(output_file, 'w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["username", "language"])

        for title, languages in filtered_results:
            language_string = json.dumps(languages, separators=(',', ':'))
            writer.writerow([title, language_string])

    print("CSV file created successfully:", output_file)


def generate_csv_files():
    language_codes, dbnames, urls = fetch_language_data()
    fetch_usernames(dbnames)

    allowed_languages = []
    for code in language_codes:
        allowed_languages.append(code)
        allowed_languages.append(f"{code}-N")
        for i in range(6):
            version = f"{code}-{i}"
            allowed_languages.append(version)
  
    for url, language_code, dbname in zip(urls, language_codes, dbnames):
        csv_file = f"/home/paws/translation-stats-data/translator_usernames/{dbname}_usernames.csv"
        output_file = f"/home/paws/translation-stats-data/translator_language_proficiency_user_template/{dbname}_translator_proficiency_user_template.csv"

        try:
            create_user_language_csv(csv_file, output_file, allowed_languages, url, dbname)
        except FileNotFoundError:
            print(f"Username file not found for {dbname}")

