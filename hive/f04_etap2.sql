use nyc_data;

CREATE OR REPLACE VIEW transformed_data AS
SELECT
    listing_id,
    calendar_date,
    CAST(REGEXP_REPLACE(REGEXP_REPLACE(price, '\\$', ''), ',', '') AS DOUBLE) AS price,
    neighbourhood_group_cleansed,
    room_type
FROM etap1_data;

CREATE OR REPLACE VIEW grouped_data AS
SELECT
    calendar_date,
    neighbourhood_group_cleansed,
    room_type,
    ROUND(AVG(price), 2) AS average_price
FROM transformed_data
GROUP BY calendar_date, neighbourhood_group_cleansed, room_type;

CREATE TABLE etap2_data AS
SELECT *
FROM grouped_data
WHERE average_price > 0.0;
