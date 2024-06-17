-- Use the nyc_data database
USE nyc_data;

-- Create an external table for holiday_dates
CREATE EXTERNAL TABLE IF NOT EXISTS holiday_dates (
                                                      holiday_date STRING
)
    USING csv
    OPTIONS (
        path '/new-york/dates.csv',
        sep ',',
        quote '\"',
        escape '\\',
        header 'true'
        );

-- Create a new table etap4_data
CREATE TABLE etap4_data AS
SELECT e.calendar_date,
       e.neighbourhood_group_cleansed,
       e.room_type,
       e.average_price,
       e.total_count AS sum,
       CASE
           WHEN dayofweek(to_date(e.calendar_date, 'yyyy-MM-dd')) IN (1, 7) THEN true
           ELSE false
           END AS is_weekend,
       IF(h.holiday_date IS NOT NULL, true, false) AS is_holiday
FROM etap3_data e
         LEFT JOIN holiday_dates h ON e.calendar_date = h.holiday_date;
