drop table if exists translators;

create table translators as
select
    distinct user_name
from translation_revisions;

-- 120,834 rows
-- 22 s
