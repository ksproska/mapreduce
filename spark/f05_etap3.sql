-- Use the nyc_data database
USE nyc_data;

-- Create a new table etap3_data by joining etap2_data with events_summed
CREATE TABLE etap3_data AS
SELECT f.calendar_date,
       f.neighbourhood_group_cleansed,
       f.room_type,
       f.average_price,
       e.total_count
FROM etap2_data f
         JOIN events_summed e
              ON f.calendar_date = e.event_date AND
                 f.neighbourhood_group_cleansed = e.event_borough;
