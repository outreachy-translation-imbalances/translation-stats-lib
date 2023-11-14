drop table if exists babel_extension_language_proficiences;

create table babel_extension_language_proficiences as
select
  t.user_name as user_name,
  b.wiki,
  collect_set(concat(b.language_used, "-", b.language_level)) as language_proficiencies
from
  translators t,
  babel_extension_user_languages b
where
  b.user_name = t.user_name
group by
  t.user_name, b.wiki;

-- 17,372 rows
-- 22 s
