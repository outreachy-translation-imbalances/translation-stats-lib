select
  event_source,
  event_subtype,
  event_type,
  human_modification_rate,
  human_modification_threshold,
  coalesce(cxsx_translation_id, translation_id) as translation_id,
  translation_provider,
  translation_type,
  wiki_db
from
  event.content_translation_event e
left join cx_section_translations
  on translation_type != 'article'
  and cxsx_id = translation_id;
