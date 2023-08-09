"""
Find and parse Template:Babel usages which intentionally avoid the Babel extension.
"""
import requests
import re
import json
from .data_store import cached
from .generate_usernames import get_userpage_titles
from .wikipedia_site_matrix import get_allowed_babel_language_proficiencies


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


def parse_babel_templates(content):
    allowed_language_proficiencies = get_allowed_babel_language_proficiencies()
    babel_templates = re.findall(r"\{\{Babel((?:(?!\{\{Babel)[^{}])+)\}\}", content, re.IGNORECASE)
    language_claims = []
    if babel_templates:
        for babel_template in babel_templates:
            user_text = str(babel_template)
            user_languages = extract_language_codes(user_text)
            valid_user_languages = set()
            for lang in user_languages:
                lang = lang.strip()
                lang = lang.rstrip('\n')
                if lang in allowed_language_proficiencies:
                    valid_user_languages.add(lang)
            language_claims.extend(valid_user_languages)
    return language_claims


def find_babel_languages(title, url):
    modified_url = url + "/w/index.php"
    params = {
        "title": title,
        "action": "raw"
    }
    content = fetch_content(modified_url, params)
    if content:
        language_claims = parse_babel_templates(content)
        return title, language_claims
    return title, []


@cached("translator_language_proficiency_misc_babel_new")
def process_data_and_create_csv(sites):
    data = []
    for site in sites:
        titles = get_userpage_titles(site["dbname"])
        results = [find_babel_languages(title, site["url"]) for title in titles]
        filtered_results = [(title, languages) for title, languages in results if languages]

        for title, languages in filtered_results:
            language_string = json.dumps(languages, separators=(',', ':'))
            data.append({
                "username": title,
                "language": language_string,
                "wikipedia": site["dbname"]
            })

    return data
