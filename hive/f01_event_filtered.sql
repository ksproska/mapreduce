use nyc_data;

CREATE EXTERNAL TABLE nyc_events_raw
(
    event_id          FLOAT,
    EventName         STRING,
    start_date        STRING,
    EndDate_Time      STRING,
    EventAgency       STRING,
    EventType         STRING,
    event_borough     STRING,
    EventLocation     STRING,
    EventStreetSide   STRING,
    StreetClosureType STRING,
    CommunityBoard    STRING,
    PolicePrecinct    STRING
)
    ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
        WITH SERDEPROPERTIES (
        "separatorChar" = ",",
        "quoteChar" = "\"",
        "escapeChar" = "\\"
        )
    STORED AS TEXTFILE
    TBLPROPERTIES ("skip.header.line.count" = "1");

LOAD DATA INPATH '/new-york/NYC_events_filtered.csv' OVERWRITE INTO TABLE nyc_events_raw;

CREATE TABLE nyc_events_filtered AS
SELECT DISTINCT event_id, start_date, event_borough
FROM nyc_events_raw;
