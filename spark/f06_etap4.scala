import scala.concurrent.duration._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

val before = System.currentTimeMillis;

val holidayDatesSchema = StructType(Array(
  StructField("holiday_date", StringType, true)
))

val holidayDates = spark.read.format("csv").option("header", "false").option("sep", ",").option("quote", "\"").option("escape", "\"").schema(holidayDatesSchema).load("/new-york/dates.csv")

val etap3Data = spark.table("etap3_data")

val etap4Data = etap3Data.withColumn("calendar_date", to_date(col("calendar_date"), "yyyy-MM-dd")).join(holidayDates.withColumnRenamed("holiday_date", "holiday"), $"calendar_date" === $"holiday", "left_outer").withColumn("is_weekend", expr("dayofweek(calendar_date) IN (1, 7)")).withColumn("is_holiday", col("holiday").isNotNull).select( $"calendar_date", $"neighbourhood_group_cleansed", $"room_type", $"average_price", $"total_count", $"is_weekend", $"is_holiday")

etap4Data.write.format("hive").mode("overwrite").saveAsTable("etap4_data")

val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"Elapsed (sec): $totalTime")
etap4Data.show

System.exit(0)
