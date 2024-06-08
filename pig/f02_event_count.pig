data_events = LOAD '/new-york/pig/events_filtered.csv' USING PigStorage(',') AS (
    event_id: float, start_date: chararray, event_borough: chararray
);
data_events = FOREACH data_events GENERATE STRSPLIT(start_date, ' ', 2).$0 AS date, event_borough, 1 AS count;

grouped_data = GROUP data_events BY (date, event_borough);
data_events_summed = FOREACH grouped_data GENERATE FLATTEN(group) AS (date, event_borough), SUM(data_events.count) AS count;

RMF /new-york/pig/events_count.csv ;
STORE data_events_summed INTO '/new-york/pig/events_count.csv' USING PigStorage(',');
