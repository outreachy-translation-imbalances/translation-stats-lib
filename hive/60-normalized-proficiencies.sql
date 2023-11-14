drop table if exists normalized_proficiencies;

create table normalized_proficiencies as (
  with merged_proficiencies as (
    select
      t.user_name,
      explode(b.language_proficiencies) as lang_proficiency
    from translators t
    join babel_extension_language_proficiences b
      on t.user_name = b.user_name

    union all

    select
      t.user_name,
      explode(u.language_proficiencies) as lang_proficiency
    from translators t
    join user_page_language_proficiencies u
      on t.user_name = u.user_name
  ),

  split_raw_proficiency as (
    select
      user_name,
      split(lang_proficiency, '-(?=[0-5N]$)', 2) as lang_proficiency
    from merged_proficiencies
  )

  select
    user_name,
    lang_proficiency[0] as lang,
    int(coalesce(nullif(max(lang_proficiency[1]), 'N'), '6')) as level
  from split_raw_proficiency
  join wikipedia_languages v
    on lang_proficiency[0] = v.lang
  group by
    user_name,
    lang_proficiency[0]
);

-- 49,613 rows, covering 10,385 users
-- 27 s

-- with en_level as (
--   select
--     user_name,
--     level
--   from normalized_proficiencies
--   where lang='en'
-- ),
-- 
-- relative_en_level as (
--   select
--     max(n.level) as highest,
--     coalesce(en.level, 0) as en_level
--   from normalized_proficiencies n
--   left join en_level en
--     on en.user_name = n.user_name
--   group by n.user_name
-- )
-- 
-- select
--   sum(case when en_level < highest then 1 else 0 end) / count(1) as en_l2_proportion
-- from relative_en_level;
-- 
