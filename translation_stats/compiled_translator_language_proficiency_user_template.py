import requests
import re
import json
from .data_store import cached
from . import generate_usernames
from . import wikipedia_site_matrix


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


@cached("translator_language_proficiency_user_template_new")
def process_data_and_create_csv(sites):
    allowed_languages = wikipedia_site_matrix.get_allowed_babel_languages()
    data = []
    for site in sites:
        titles = generate_usernames.get_userpage_titles(site["dbname"])
        results = [find_babel_languages(title, allowed_languages, site["url"], site["dbname"]) for title in titles]
        filtered_results = [(title, languages) for title, languages in results if languages]

        for title, languages in filtered_results:
            language_string = json.dumps(languages, separators=(',', ':'))
            data.append({
                "username": title,
                "language": language_string,
                "wikipedia": site["dbname"]
            })

    return data


def generate_csv_files():
    process_data_and_create_csv(wikipedia_site_matrix.get_wikipedias())
