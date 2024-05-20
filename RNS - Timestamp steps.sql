with ga as (
  select
    date,
    CONCAT(CAST(visitId AS STRING),'_',fullVisitorId) AS sessionId,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=1) AS GUID,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=74) AS Brand,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=45) AS Customer_Status,
    (SELECT value FROM UNNEST(hits.customDimensions) WHERE index=104) AS Reg_Step,
    geoNetwork.country as Country,
    channelGrouping,
    concat(trafficsource.source," / ",trafficsource.medium) as Source_Medium,
    hits.time as hit_ts,
    hits.eventInfo.eventAction as EA,
    hits.eventInfo.eventCategory as EC
  from `steam-mantis-108908.136528711.ga_sessions_*`
  left join unnest(hits) AS hits
  where hits.type = 'EVENT' and
  _table_suffix between '20231107' and '20231116' and
  hits.eventInfo.eventCategory in ('Registration Funnel') and hits.time > 0
),
zeroth_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as quote_ts
  from ga
  where EA = 'Registration Form Open'
  group by 1,2
),
first_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as app_complete_ts
  from ga
  where EC = 'Registration Funnel' AND Reg_Step = "Step 1 (personal group - name)"
  group by 1,2
),
second_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as app_complete_ts
  from ga
  where EC = 'Registration Funnel' AND Reg_Step = "Step 2 (personal group - password)"
  group by 1,2
),
third_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as app_complete_ts
  from ga
  where EC = 'Registration Funnel' AND Reg_Step = "Step 3 (address group - address)"
  group by 1,2
),
fourth_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as app_complete_ts
  from ga
  where EC = 'Registration Funnel' AND Reg_Step = "Step 4 (extra group - phone)"
  group by 1,2
),
fifth_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as app_complete_ts
  from ga
  where EC = 'Registration Funnel' AND Reg_Step = "Step 5 (extra group - additional)"
  group by 1,2
),
sixth_event as (
  select
    sessionId,
    channelGrouping,
    min(hit_ts) as app_complete_ts
  from ga
  where EC = 'Registration Funnel' AND Reg_Step = "Step 6 (extra group - deposit-limit)"
  group by 1,2
),
seventh_event as (
  select
    sessionId,date,
    channelGrouping,
    GUID, Brand, Customer_Status, Source_Medium,Country,
    min(hit_ts) as app_complete_ts
  from ga
  where EA = 'Registration Complete'
  group by 1,2,3,4,5,6,7,8
),
joined as (
  select
    date,sessionID,
    channelGrouping,Source_Medium,Country,
    GUID, Brand, Customer_Status,
    zeroth_event.quote_ts as F0,
    first_event.app_complete_ts as F1,
    second_event.app_complete_ts as F2,
    third_event.app_complete_ts as F3,
    fourth_event.app_complete_ts as F4,
    fifth_event.app_complete_ts as F5,
    sixth_event.app_complete_ts as F6,
    seventh_event.app_complete_ts as F7
  from zeroth_event inner join first_event using(sessionID,channelGrouping)
  inner join second_event using(sessionID,channelGrouping) inner join third_event using(sessionID,channelGrouping) inner join fourth_event using(sessionID,channelGrouping) inner join fifth_event using(sessionID,channelGrouping) inner join sixth_event using(sessionID,channelGrouping) inner join seventh_event using(sessionID,channelGrouping)
)

SELECT date,Country,joined.GUID, Brand, Customer_Status,joined.F0 as Reg_Form_Open,joined.F1 as Step1,joined.F2 as Step2,joined.F3 as Step3,joined.F4 as Step4,joined.F5 as Step5,joined.F6 as Step6,joined.F7 as NRC
FROM joined
WHERE
Customer_Status = 'Prospect'