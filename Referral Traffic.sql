with date_range as (
  select
    '20190101' as start_date,
    '20230430' as end_date)
select date, Brand, Country, source, default_channel_grouping, sum(sessions) as sessions, sum(bounce) as Bounce, sum(new_visits) as NewVisits, sum(Pageviews) as Pageviews, avg(session_quality_dim) as sessionQualityDim from(
select
  date,
  geoNetwork.country	as Country,
  (select value from unnest(hits.customdimensions) where index = 74 group by value) as Brand,
  trafficsource.referralpath as referral_path,
  concat(trafficsource.source,trafficsource.referralpath) as full_referrer,
  channelgrouping as default_channel_grouping,
  trafficsource.campaign as campaign,
  trafficsource.source as source,
  trafficsource.medium as medium,
  concat(trafficsource.source," / ",trafficsource.medium) as source_medium,
  trafficsource.keyword as keyword,
  trafficsource.campaigncode as campaign_code,
  totals.visits as sessions,
  totals.newVisits as new_visits,
  totals.pageviews as Pageviews,
  totals.sessionQualityDim as session_quality_dim,
  totals.bounces as bounce


from
  `steam-mantis-108908.88654663.ga_sessions_*` as sessions,
  unnest(hits) as hits,
  date_range
where
  _table_suffix between date_range.start_date and date_range.end_date
  and hits.type in ('PAGE','EVENT')
  and totals.visits = 1) where regexp_contains(medium, 'referral') and Country = "Sweden" group by 1, 2, 3, 4, 5