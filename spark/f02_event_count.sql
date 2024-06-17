-- Use the nyc_data database
USE nyc_data;

-- Create or replace a view for transformed events
CREATE OR REPLACE VIEW events_transformed AS
SELECT split(start_date, ' ')[0] AS event_date,
       event_borough,
       1 AS count
FROM nyc_events_filtered;

-- Create a table to sum counts by event_date and event_borough
CREATE TABLE events_summed AS
SELECT event_date,
       event_borough,
       SUM(count) AS total_count
FROM events_transformed
GROUP BY event_date, event_borough;
