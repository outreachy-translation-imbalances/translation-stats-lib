drop table if exists translator_user_pages;

create table translator_user_pages as
select
  wiki_db,
  page_id,
  page_title,
  regexp_replace(page_title, '^[^:]+:', '') as user_name,
  revision_id,
  revision_text,
  revision_timestamp
from translators t, wmf.mediawiki_wikitext_current
where
  regexp_replace(page_title, '^[^:]+:', '') = t.user_name
  and snapshot = '${snapshot}'
  and page_namespace = 2;

-- 179,557 rows
-- 278 s
