with date_range as (
  select
    '20190101' as start_date,
    '20230430' as end_date)
SELECT date, Brand, Country, error_type as Error_Type, Area as Error, sum(pageviews) as Pageviews from  
(SELECT
  date,
  (select value from unnest(hits.customdimensions) where index = 74 group by value) as Brand,
  geoNetwork.country	as Country,
  hits.page.pageTitle as error_type,
  (select value from unnest(hits.customdimensions) where index = 55 group by value) as Area,
  hits.page.pagePath as Page,
  totals.pageviews as pageviews
from
  `steam-mantis-108908.136528711.ga_sessions_*` as sessions,
  unnest(hits) as hits,
  date_range
where
  _table_suffix between date_range.start_date and date_range.end_date
  and hits.type in ('PAGE','EVENT')
  and totals.visits = 1
group by date, Country, Brand, Page, Area, Pageviews, error_type) where Area = "Promotions" and Country = "Sweden" group by 1,2,3,4,5