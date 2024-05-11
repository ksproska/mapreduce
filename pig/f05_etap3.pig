data_events = LOAD '/new-york/pig/events_count.csv' USING PigStorage(',') AS (
    date: chararray, neighbourhood: chararray, sum: chararray
);

data_phase2 = LOAD '/new-york/pig/etap2.csv' USING PigStorage(',') AS (
    date: chararray, neighbourhood_group_cleansed: chararray, room_type: chararray, average_price: chararray
);

--data_events_head = LIMIT data_events 10;
--DUMP data_events_head;
--data_phase2_head = LIMIT data_phase2 10;
--DUMP data_phase2_head;

data_phase3 = JOIN data_events BY (date, neighbourhood), data_phase2 BY (date, neighbourhood_group_cleansed);
data_phase3 = FOREACH data_phase3 GENERATE data_phase2::date, data_phase2::neighbourhood_group_cleansed,
data_phase2::room_type, data_phase2::average_price, data_events::sum;
--data_phase3_head = LIMIT data_phase3 10;
--DUMP data_phase3_head;

RMF /new-york/pig/etap3.csv ;
STORE data_phase3 INTO '/new-york/pig/etap3.csv' USING PigStorage(',');
