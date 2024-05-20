with date_range as (
  select
    '20231018' as start_date,
    '20231019' as end_date),

avg_time as (
select
  pagepath as page,
  pagetitle,
  case when pageviews = exits then 0 else total_time_on_page / (pageviews - exits) end as avg_time_on_page
from (
  select
    pagepath,
    pagetitle,
    count(*) as pageviews,
    countif(isexit is not null) as exits,
    sum(time_on_page) as total_time_on_page
  from (
    select
      fullvisitorid,
      visitstarttime,
      pagepath,
      pagetitle,
      hit_time,
      type,
      isexit,
      case when isexit is not null then last_interaction - hit_time else next_pageview - hit_time end as time_on_page
    from (
      select
        fullvisitorid,
        visitstarttime,
        hits.page.pagepath,
        hits.page.pagetitle,
        hits.time / 1000 as hit_time,
        hits.type,
        hits.isexit,
        max(if(hits.isinteraction = true,hits.time / 1000,0)) over (partition by fullvisitorid, visitstarttime) as last_interaction,
        lead(hits.time / 1000) over (partition by fullvisitorid, visitstarttime order by hits.time / 1000) as next_pageview
      from
        `steam-mantis-108908.136528711.ga_sessions_*`,
        unnest(hits) as hits,
        date_range
      where
        _table_suffix between start_date and end_date
        and hits.type = 'PAGE'
        and totals.visits = 1))
  group by
    pagepath,
    pagetitle))
      
select
  hits.page.pagepath as page,
  -- hits.page.pagetitle as page_title,
  count(*) as pageviews,
  count(distinct concat(cast(fullvisitorid as string), cast(visitstarttime as string))) as unique_pageviews,
  avg_time_on_page,
  countif(hits.isentrance = true) as entrances,
  countif(totals.bounces = 1) / count(distinct concat(fullvisitorid, cast(visitstarttime as string))) as bounce_rate,
  countif(hits.isexit = true) / count(*) as exit_rate
from
  `steam-mantis-108908.136528711.ga_sessions_*` as sessions,
  unnest(hits) as hits,
  date_range
  left join avg_time on hits.page.pagepath = avg_time.page
  and hits.page.pagetitle = avg_time.pagetitle
where
  _table_suffix between start_date and end_date
  and totals.visits = 1
  and hits.type = 'PAGE'
group by
  page,
  avg_time_on_page
  -- ,page_title
order by
  pageviews desc