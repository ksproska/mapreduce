import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val before = System.currentTimeMillis;

val schema = StructType(Array(
  StructField("event_id", FloatType, true),
  StructField("EventName", StringType, true),
  StructField("start_date", StringType, true),
  StructField("EndDate_Time", StringType, true),
  StructField("EventAgency", StringType, true),
  StructField("EventType", StringType, true),
  StructField("event_borough", StringType, true),
  StructField("EventLocation", StringType, true),
  StructField("EventStreetSide", StringType, true),
  StructField("StreetClosureType", StringType, true),
  StructField("CommunityBoard", StringType, true),
  StructField("PolicePrecinct", StringType, true)
))

val rawData = spark.read.format("csv").option("header", "true").option("sep", ",").option("quote", "\"").option("escape", "\"").schema(schema).load("/new-york/NYC_events_filtered.csv")

val filteredEvents = rawData.select("event_id", "start_date", "event_borough").distinct()

filteredEvents.write.format("hive").mode("overwrite").saveAsTable("nyc_events_filtered")

val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"f01_event_filtered - Elapsed (sec): $totalTime")
filteredEvents.show

System.exit(0)
