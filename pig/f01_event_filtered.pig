data_events = LOAD '/new-york/NYC_events_filtered.csv' USING PigStorage(',') AS (
    event_id: float, EventName: chararray, start_date: chararray, EndDate_Time: chararray, EventAgency: chararray,
    EventType: chararray, event_borough: chararray, EventLocation: chararray, EventStreetSide: chararray,
    StreetClosureType: chararray, CommunityBoard: chararray, PolicePrecinct: chararray
);
data_events = FOREACH data_events GENERATE event_id, start_date, event_borough;

data_events_distinct = DISTINCT data_events;
--data_events_distinct_group = GROUP data_events_distinct ALL;
--
--data_events_group = GROUP data_events ALL;
--
--events_count = FOREACH data_events_group GENERATE COUNT(data_events) AS events_count;
--events_distinct_count = FOREACH data_events_distinct_group GENERATE COUNT(data_events_distinct) AS events_distinct_count;

STORE data_events_distinct INTO '/new-york/pig/events_filtered.csv' USING PigStorage(',');
