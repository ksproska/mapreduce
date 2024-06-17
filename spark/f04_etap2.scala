import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.concurrent.duration._

val before = System.currentTimeMillis;

val etap1Data = spark.table("etap1_data")

val transformedData = etap1Data.withColumn("price", regexp_replace(regexp_replace(col("price"), "\\$", ""), ",", "").cast("double")).select("listing_id", "calendar_date", "price", "neighbourhood_group_cleansed", "room_type")
val groupedData = transformedData.groupBy("calendar_date", "neighbourhood_group_cleansed", "room_type").agg(round(avg("price"), 2).alias("average_price"))

val etap2Data = groupedData.filter(col("average_price") > 0.0)

etap2Data.write.format("hive").mode("overwrite").saveAsTable("etap2_data")


val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"Elapsed (sec): $totalTime")
System.exit(0)
