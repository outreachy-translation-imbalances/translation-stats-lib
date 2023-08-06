import requests
import csv
import re
import json
from .data_store import cached


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


@cached("/home/paws/translation-stats-data/translator_language_proficiency_user_template_new")
def process_data_and_create_csv(allowed_languages, urls, dbnames):
    csv_data = []
    for url, dbname in zip(urls, dbnames):
        csv_file = f"/home/paws/translation-stats-data/translator_usernames/{dbname}_usernames.csv"

        try:
            titles = get_user_titles_with_babel_from_csv(csv_file)
            results = [find_babel_languages(title, allowed_languages, url, dbname) for title in titles]
            filtered_results = [(title, languages) for title, languages in results if languages]

            for title, languages in filtered_results:
                language_string = json.dumps(languages, separators=(',', ':'))
                # writer.writerow([title, language_string])
                csv_data.append({"username": title, "language": language_string, "wikipedia": dbname})

        except FileNotFoundError:
            print(f"Username file not found for {dbname}")
    return csv_data


def generate_csv_files():
    language_codes = []
    dbnames = []
    urls = []

    language_data = "/home/paws/translation-stats-data/language_data.csv"

    with open(language_data, 'r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            language_codes.append(row['Language Code'])
            dbnames.append(row['DB Name'])
            urls.append(row['URL'])

    allowed_languages = []
    for code in language_codes:
        allowed_languages.append(code)
        allowed_languages.append(f"{code}-N")
        for i in range(6):
            version = f"{code}-{i}"
            allowed_languages.append(version)

    process_data_and_create_csv(allowed_languages, urls, dbnames)

