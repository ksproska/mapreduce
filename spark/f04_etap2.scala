import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.concurrent.duration._

val before = System.currentTimeMillis;

//val etap1Data = spark.table("etap1_data")
//
//val etap1Grouped = etap1Data.groupBy("date", "neighbourhood_group_cleansed", "room_type")




val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"Elapsed (sec): $totalTime")
System.exit(0)
