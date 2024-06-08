USE nyc_data;

CREATE EXTERNAL TABLE IF NOT EXISTS holiday_dates
(
    holiday_date STRING
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar" = "\"",
        "escapeChar" = "\\"
        )
    STORED AS TEXTFILE;
LOAD DATA INPATH '/new-york/dates.csv' OVERWRITE INTO TABLE holiday_dates;

SELECT * FROM holiday_dates;

CREATE TABLE etap4_data AS
SELECT e.calendar_date,
       e.neighbourhood_group_cleansed,
       e.room_type,
       e.average_price,
       e.total_count                               AS sum,
       CASE
           WHEN dayofweek(from_unixtime(unix_timestamp(e.calendar_date, 'yyyy-MM-dd'))) IN (1, 7) THEN true
           ELSE false END                          AS is_weekend,
       IF(h.holiday_date IS NOT NULL, true, false) AS is_holiday
FROM etap3_data e
         LEFT JOIN
     holiday_dates h ON e.calendar_date = h.holiday_date;

SELECT * FROM etap4_data;
