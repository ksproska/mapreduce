use nyc_data;

CREATE OR REPLACE VIEW events_transformed AS
SELECT split(start_date, ' ')[0] AS event_date,
       event_borough,
       1                         AS count
FROM nyc_events_filtered;


CREATE TABLE events_summed AS
SELECT event_date,
       event_borough,
       SUM(count) AS total_count
FROM events_transformed
GROUP BY event_date, event_borough;
