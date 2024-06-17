import scala.concurrent.duration._
import org.apache.spark.sql.functions._

val before = System.currentTimeMillis;

val etap2Data = spark.table("etap2_data")
val eventsSummed = spark.table("events_summed")

val etap3Data = etap2Data.alias("f").join(eventsSummed.alias("e"), col("f.calendar_date") === col("e.event_date") && col("f.neighbourhood_group_cleansed") === col("e.event_borough")).select(col("f.calendar_date"), col("f.neighbourhood_group_cleansed"), col("f.room_type"), col("f.average_price"), col("e.total_count"))

etap3Data.write.format("hive").mode("overwrite").saveAsTable("etap3_data")

val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"f05_etap3 - Elapsed (sec): $totalTime")
etap3Data.show

System.exit(0)
