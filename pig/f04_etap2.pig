data_phase1 = LOAD '/new-york/pig/etap1.csv' USING PigStorage(',') AS (
    listing_id: chararray, date: chararray, price: chararray, neighbourhood_group_cleansed: chararray, room_type: chararray
);
data_phase1 = FOREACH data_phase1 GENERATE listing_id, date, (double) REPLACE(REPLACE(price, '\\\$', ''), ',', '') AS price,
neighbourhood_group_cleansed, room_type;

data_phase1 = GROUP data_phase1 BY (date, neighbourhood_group_cleansed, room_type);

data_phase1 = FOREACH data_phase1 GENERATE
    group.date AS date,
    group.neighbourhood_group_cleansed AS neighbourhood_group_cleansed,
    group.room_type AS room_type,
    ROUND(AVG(data_phase1.price) * 100) / 100.0 AS average_price;

data_phase1 = FILTER data_phase1 BY average_price > 0.0;

RMF /new-york/pig/etap2.csv ;
STORE data_phase1 INTO '/new-york/pig/etap2.csv' USING PigStorage(',');
