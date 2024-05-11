data_weather = LOAD '/new-york/weather_data.csv' USING PigStorage(',') AS (
    date: chararray, temperature: double, weather_conditions: chararray
);

data_events = LOAD '/new-york/pig/etap4.csv' USING PigStorage(',') AS (
    date: chararray, neighbourhood_group_cleansed: chararray, room_type: chararray, average_price: chararray,
    sum: chararray, is_weekend: chararray, is_holiday: chararray
);

merged_data = JOIN data_events BY date, data_weather BY date;
merged_data = FOREACH merged_data GENERATE data_events::date, data_events::neighbourhood_group_cleansed,
    data_events::room_type, data_events::average_price, data_events::sum, data_events::is_weekend, data_events::is_holiday,
    data_weather::temperature, data_weather::weather_conditions;


--data_phase5_head = LIMIT merged_data 10;
--DUMP data_phase5_head;

RMF /new-york/pig/etap5.csv ;
STORE merged_data INTO '/new-york/pig/etap5.csv' USING PigStorage(',');
