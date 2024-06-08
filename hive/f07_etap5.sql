CREATE EXTERNAL TABLE IF NOT EXISTS data_weather
(
    weather_date               STRING,
    temperature        DOUBLE,
    weather_conditions STRING
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
            "separatorChar" = ",",
            "quoteChar" = "\"",
            "escapeChar" = "\\"
            )
        STORED AS TEXTFILE;
LOAD DATA INPATH '/new-york/weather_data.csv' OVERWRITE INTO TABLE data_weather;

SELECT * FROM data_weather;

CREATE TABLE etap5_data AS
SELECT d.calendar_date,
       d.neighbourhood_group_cleansed,
       d.room_type,
       d.average_price,
       d.sum,
       d.is_weekend,
       d.is_holiday,
       w.temperature,
       w.weather_conditions
FROM etap4_data d
         JOIN
     data_weather w ON d.calendar_date = w.weather_date;


SELECT count(*) FROM etap5_data;