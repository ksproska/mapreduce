import scala.concurrent.duration._
import org.apache.spark.sql.types._

val before = System.currentTimeMillis;

val etap4Data = spark.table("etap4_data")

val weatherSchema = StructType(Array(
  StructField("date", StringType, true),
  StructField("temperature", StringType, true),
  StructField("conditions", StringType, true)
))

val weather = spark.read.format("csv").option("header", "false").option("sep", ",").option("quote", "\"").option("escape", "\"").schema(weatherSchema).load("/new-york/weather_data.csv")

val etap5Data = etap4Data.alias("f").join(weather.alias("e"), col("f.calendar_date") === col("e.date")).select(col("f.calendar_date"), col("f.neighbourhood_group_cleansed"), col("f.room_type"), col("f.average_price"), col("f.total_count"), col("f.is_weekend"), col("f.is_holiday"), col("e.temperature"), col("e.conditions"))
etap5Data.write.format("hive").mode("overwrite").saveAsTable("etap5_data")
etap5Data.show

val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"f07_etap5 - Elapsed (sec): $totalTime")
System.exit(0)
