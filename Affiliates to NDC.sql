with ga as (
  select
    CONCAT(CAST(visitId AS STRING),'_',fullVisitorId) AS sessionId,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1) AS GUID,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=74) AS Brand,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=45) AS Customer_Status,
    channelGrouping,
    trafficsource.source as Source,
    trafficsource.medium as Medium,
    hits.time as hit_ts,
    hits.eventInfo.eventAction as EA
  from `steam-mantis-108908.136528711.ga_sessions_*`
  left join unnest(hits) AS hits
  where hits.type = 'EVENT' and
  _table_suffix between '20230101' and '20230430' and
  hits.eventInfo.eventAction in ('Registration Form Open','Registration Complete') and hits.time > 0
),
first_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as quote_ts
  from ga
  where EA = 'Registration Form Open'
  group by 1,2
),
final_event as (
  select
    sessionId,
    channelGrouping,
    GUID, Brand, Customer_Status, Source, Medium,
    min(hit_ts) as app_complete_ts
  from ga
  where EA = 'Registration Complete'
  group by 1,2,3,4,5,6,7
),
joined as (
  select
    sessionID,
    channelGrouping,Source, Medium,
    GUID, Brand, Customer_Status,
    (final_event.app_complete_ts - first_event.quote_ts) AS Time_Taken_Reg_open_to_Reg_Success
  from first_event 
  inner join final_event using(sessionID,channelGrouping)
  Where final_event.app_complete_ts > first_event.quote_ts
),

NDC as (
Select
    hits.eventInfo.eventAction as EA,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1) AS GUID
from `steam-mantis-108908.136528711.ga_sessions_*`
left join unnest(hits) AS hits
where hits.type = 'EVENT' and
_table_suffix between '20230101' and '20230430' AND 
hits.eventInfo.eventAction = "NDC"
)

SELECT channelGrouping,Source,Medium, Brand, (AVG(Time_Taken_Reg_open_to_Reg_Success)/60000) as Time_Taken_Reg_open_to_Reg_Success
FROM joined
JOIN NDC ON joined.GUID = NDC.GUID
WHERE
Customer_Status = 'Prospect' AND channelGrouping = 'Affiliates'AND Joined.GUID !='00000000-0000-0000-0000-000000000000'
Group BY 1,2,3,4