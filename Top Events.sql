select
  hits.eventinfo.eventcategory as event_category,
  hits.eventinfo.eventaction as event_action,
  hits.eventinfo.eventlabel as event_label,
  count(*) as total_events,
  count(distinct concat(cast(fullvisitorid as string), cast(visitstarttime as string))) as unique_events,
  ifnull(sum(hits.eventinfo.eventvalue),0) as event_value,
  ifnull(sum(hits.eventinfo.eventvalue) / count(*),0) as avg_value
from
  `steam-mantis-108908.136528711.ga_sessions_*`,
  unnest(hits) as hits
where
  totals.visits = 1
  and hits.type = 'EVENT' AND
  _table_suffix between '20231018' and '20231019'
  and hits.eventinfo.eventcategory is not null
group by
  event_category
  ,event_action
  , event_label
order by total_events desc