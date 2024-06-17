import org.apache.spark.sql.functions._

val before = System.currentTimeMillis;

val filteredEvents = spark.table("nyc_events_filtered")

val eventsTransformed = filteredEvents.withColumn("event_date", split(col("start_date"), " ")(0)).withColumn("count", lit(1)).select("event_date", "event_borough", "count")

val eventsSummed = eventsTransformed.groupBy("event_date", "event_borough").agg(sum("count").alias("total_count"))

eventsSummed.write.format("hive").mode("overwrite").saveAsTable("events_summed")

val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"Elapsed (sec): $totalTime")
eventsSummed.show

System.exit(0)
