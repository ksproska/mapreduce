DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();
data_events = LOAD '/new-york/NYC_events_filtered.csv' USING CSVLoader(',') AS (
    event_id: float, EventName: chararray, start_date: chararray, EndDate_Time: chararray, EventAgency: chararray,
    EventType: chararray, event_borough: chararray, EventLocation: chararray, EventStreetSide: chararray,
    StreetClosureType: chararray, CommunityBoard: chararray, PolicePrecinct: chararray
);
data_events = FOREACH data_events GENERATE event_id, start_date, event_borough;
data_events = DISTINCT data_events;

RMF /new-york/pig/events_filtered.csv ;
STORE data_events INTO '/new-york/pig/events_filtered.csv' USING PigStorage(',');
