-- Collect the translator's proficiency level in the source and target languages
-- for each translation.

drop table if exists translation_proficiency_pairs;

create table translation_proficiency_pairs as
select
  source_language,
  target_language,
  s.level as source_level,
  t.level as target_level
from
  normalized_proficiencies s,
  normalized_proficiencies t,
  translation_revisions cx
where
  s.user_name = cx.user_name
  and s.lang = cx.source_language
  and t.user_name = cx.user_name
  and t.lang = cx.target_language;

-- 355,487 rows
-- 21 s

-- select sum(case when (target_level > source_level) then 1 else 0 end) from translation_proficiency_pairs;
-- 283,834 -> 80% are translating into L1

-- select
--   sum(case when (target_level > source_level) then 1 else 0 end) / count(1)
--     as l1_proportion,
--   target_language='en' as is_to_english
-- from translation_proficiency_pairs
-- group by target_language='en';
-- 
-- l1_proportion   is_to_english
-- 0.30 true
-- 0.81 false
