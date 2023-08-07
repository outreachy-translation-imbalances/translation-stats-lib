import requests
import re
import json

from .data_store import cached
from .generate_usernames import get_userpage_titles
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


def parse_babel_templates(content, dbname):
    allowed_languages = wikipedia_site_matrix.get_allowed_babel_languages()
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


def find_babel_languages(title, url, dbname):
    modified_url = url + "/w/index.php"
    params = {
        "title": title,
        "action": "raw"
    }
    content = fetch_content(modified_url, params)
    if content:
        language_claims = parse_babel_templates(content, dbname)
        return title, language_claims
    return title, []


@cached("translator_language_proficiency_user_template/{dbname}_translator_proficiency_user_template")
def create_user_language_csv(*, dbname, url):
    titles = get_userpage_titles(dbname)
    results = [find_babel_languages(title, url, dbname) for title in titles]
    filtered_results = [(title, languages) for title, languages in results if languages]

    data = []
    for title, languages in filtered_results:
        language_string = json.dumps(languages, separators=(',', ':'))
        data.append({
            "username": title,
            "languages": language_string
        })

    return data


def generate_csv_files():
    for site in wikipedia_site_matrix.get_wikipedias():
        create_user_language_csv(dbname=site['dbname'], url=site['url'])
