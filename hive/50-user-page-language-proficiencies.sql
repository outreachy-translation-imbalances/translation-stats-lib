drop table if exists user_page_language_proficiencies;

create table user_page_language_proficiencies as (
  with user_page_templates as (
    select
      wiki_db as wiki,
      user_name,
      regexp_extract(
        revision_text,
        '(?isx)
          [{][{]
          (?: babel | user | utilisateur )\\b
          \\s*[|]\\s*
          ([^{}]+?)\\s*
          }}'
      ) as template_params
    from translator_user_pages
  )

  select
    wiki,
    user_name,
    split(template_params, '\\s*[|]\\s*', -1) as language_proficiencies
  from user_page_templates
  where template_params != ""
);

-- 17,345 rows
-- 20 s
