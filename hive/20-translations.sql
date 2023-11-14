drop table if exists translation_revisions;

create table translation_revisions as
select
    wiki_db,
    page_id,
    revision_id,
    event_user_text as user_name,
    source_language,
    target_language,
    cx_version
from
  wmf.mediawiki_history,
  cx_translations cx
where
  target_revision_id = revision_id
  and wiki_db = concat(target_language, "wiki")
  and event_entity = 'revision'
  and snapshot = '${snapshot}';

-- Note that this could be time-windowed to synchronize incrementally.

-- 1,587,521 rows
-- 643 s

-- select cx_version, count(1) from translation_revisions group by cx_version;
-- cx_version      count(1)
-- 1       347456
-- 2       1220426
-- 3       19639
-- 
-- select cx_version, sum(case when source_language = 'en' then 1 else 0 end) / count(1) as source_en_proportion from translation_revisions group by cx_version;
-- cx_version      source_en_proportion
-- 1       0.64
-- 2       0.75
-- 3       0.82
