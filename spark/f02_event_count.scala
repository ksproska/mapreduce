import org.apache.spark.sql.functions._

val filteredEvents = spark.table("nyc_events_filtered")

val eventsTransformed = filteredEvents.withColumn("event_date", split(col("start_date"), " ")(0)).withColumn("count", lit(1)).select("event_date", "event_borough", "count")

val eventsSummed = eventsTransformed.groupBy("event_date", "event_borough").agg(sum("count").alias("total_count"))

eventsSummed.write.format("hive").mode("overwrite").saveAsTable("events_summed")
