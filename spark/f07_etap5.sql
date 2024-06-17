-- Use the nyc_data database
USE nyc_data;

-- Create an external table for weather data
CREATE EXTERNAL TABLE IF NOT EXISTS data_weather (
                                                     weather_date       STRING,
                                                     temperature        DOUBLE,
                                                     weather_conditions STRING
)
    USING csv
    OPTIONS (
        path '/new-york/weather_data.csv',
        sep ',',
        quote '\"',
        escape '\\',
        header 'true'
        );

-- Create a new table etap5_data by joining etap4_data with data_weather
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
         JOIN data_weather w ON d.calendar_date = w.weather_date;
