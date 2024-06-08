use nyc_data;

CREATE TABLE etap3_data AS
SELECT f.calendar_date,
       f.neighbourhood_group_cleansed,
       f.room_type,
       f.average_price,
       e.total_count
FROM etap2_data f
         JOIN
     events_summed e
     ON
         f.calendar_date = e.event_date AND
         f.neighbourhood_group_cleansed = e.event_borough;

