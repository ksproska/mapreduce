data_events = LOAD '/new-york/pig/etap3.csv' USING PigStorage(',') AS (
    date: datetime, neighbourhood_group_cleansed: chararray, room_type: chararray, average_price: chararray, sum: chararray
);
holiday_dates = LOAD '/new-york/dates.csv' USING PigStorage(',') AS (date: chararray);

data_events = FOREACH data_events GENERATE
    ToString(date, 'yyyy-MM-dd') as date,
    neighbourhood_group_cleansed,
    room_type,
    average_price,
    sum,
    (((ToString(date, 'EEE') == 'Sat') or (ToString(date, 'EEE') == 'Sun')) ? true : false) AS is_weekend;

joined_data = JOIN data_events BY date LEFT OUTER, holiday_dates BY date;
data_phase4 = FOREACH joined_data GENERATE
    data_events::date AS date,
    neighbourhood_group_cleansed,
    room_type,
    average_price,
    sum,
    is_weekend,
    (holiday_dates::date IS NOT NULL ? true : false) AS is_holiday;

RMF /new-york/pig/etap4.csv ;
STORE data_phase4 INTO '/new-york/pig/etap4.csv' USING PigStorage(',');
