-- Use the nyc_data database
USE nyc_data;

-- Create an external table
CREATE EXTERNAL TABLE nyc_events_raw (
                                         event_id FLOAT,
                                         EventName STRING,
                                         start_date STRING,
                                         EndDate_Time STRING,
                                         EventAgency STRING,
                                         EventType STRING,
                                         event_borough STRING,
                                         EventLocation STRING,
                                         EventStreetSide STRING,
                                         StreetClosureType STRING,
                                         CommunityBoard STRING,
                                         PolicePrecinct STRING
)
    USING csv
OPTIONS (
    path '/new-york/NYC_events_filtered.csv',
    sep ',',
    quote '\"',
    escape '\\',
    header 'true'
);

-- Create a table with distinct values
CREATE TABLE nyc_events_filtered AS
SELECT DISTINCT event_id, start_date, event_borough
FROM nyc_events_raw;
