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


def parse_babel_templates(content):
    allowed_languages = wikipedia_site_matrix.get_allowed_babel_languages()
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
                if lang in allowed_languages:
                    valid_user_languages.add(lang)
            language_claims.extend(valid_user_languages)
    return language_claims


def _find_babel_languages(dbname, url, username):
    userpage = generate_usernames.get_userpage(dbname=dbname, url=url, username=username)
    language_claims = parse_babel_templates(userpage['wikitext'])
    return username, language_claims


@cached("translator_language_proficiency_misc_babel_new")
def process_data_and_create_csv(sites):
    data = []
    for site in sites:
        users = generate_usernames.get_translators(wiki=site["dbname"])
        results = [_find_babel_languages(site["dbname"], site["url"], user['username']) for user in users]
        filtered_results = [(user, languages) for user, languages in results if languages]

        for user, languages in filtered_results:
            language_string = json.dumps(languages, separators=(',', ':'))
            data.append({
                "username": user,
                "language": language_string,
                "wikipedia": site["dbname"]
            })

    return data


def generate_csv_files():
    process_data_and_create_csv(wikipedia_site_matrix.get_wikipedias())

