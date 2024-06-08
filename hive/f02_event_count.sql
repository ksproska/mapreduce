CREATE OR REPLACE VIEW events_transformed AS
SELECT split(start_date, ' ')[0] AS event_date,
       event_borough,
       1                         AS count
FROM nyc_events_filtered;

SELECT * FROM events_transformed;

DROP TABLE IF EXISTS events_summed;
CREATE TABLE events_summed AS
SELECT event_date,
       event_borough,
       SUM(count) AS total_count
FROM events_transformed
GROUP BY event_date, event_borough;

SELECT * FROM events_summed LIMIT 10;
