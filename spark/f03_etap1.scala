import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.concurrent.duration._

val before = System.currentTimeMillis;

val calendarSchema = StructType(Array(
  StructField("listing_id", StringType, true),
  StructField("calendar_date", StringType, true),
  StructField("available", StringType, true),
  StructField("price", StringType, true),
  StructField("adjusted_price", StringType, true),
  StructField("minimum_nights", StringType, true),
  StructField("maximum_nights", StringType, true)
))

val calendarData = spark.read.format("csv").option("header", "true").option("sep", ",").option("quote", "\"").option("escape", "\\").schema(calendarSchema).load("/new-york/calendar.csv")

val listingsDetailedSchema = StructType(Array(
  StructField("id", StringType, true),
  StructField("listing_url", StringType, true),
  StructField("scrape_id", StringType, true),
  StructField("last_scraped", StringType, true),
  StructField("source", StringType, true),
  StructField("name", StringType, true),
  StructField("description", StringType, true),
  StructField("neighborhood_overview", StringType, true),
  StructField("picture_url", StringType, true),
  StructField("host_id", StringType, true),
  StructField("host_url", StringType, true),
  StructField("host_name", StringType, true),
  StructField("host_since", StringType, true),
  StructField("host_location", StringType, true),
  StructField("host_about", StringType, true),
  StructField("host_response_time", StringType, true),
  StructField("host_response_rate", StringType, true),
  StructField("host_acceptance_ra", StringType, true),
  StructField("host_is_superhost", StringType, true),
  StructField("host_thumbnail_url", StringType, true),
  StructField("host_picture_url", StringType, true),
  StructField("host_neighbourhood", StringType, true),
  StructField("host_listings_count", StringType, true),
  StructField("host_total_listings_count", StringType, true),
  StructField("host_verifications", StringType, true),
  StructField("host_has_profile_pic", StringType, true),
  StructField("host_identity_verified", StringType, true),
  StructField("neighbourhood", StringType, true),
  StructField("neighbourhood_cleansed", StringType, true),
  StructField("neighbourhood_group_cleansed", StringType, true),
  StructField("latitude", StringType, true),
  StructField("longitude", StringType, true),
  StructField("property_type", StringType, true),
  StructField("room_type", StringType, true),
  StructField("accommodates ", StringType, true),
  StructField("bathrooms", StringType, true),
  StructField("bathrooms_text", StringType, true),
  StructField("bedrooms", StringType, true),
  StructField("beds", StringType, true),
  StructField("amenities", StringType, true),
  StructField("price", StringType, true),
  StructField("minimum_nights", StringType, true),
  StructField("maximum_nights", StringType, true),
  StructField("minimum_minimum_nights", StringType, true),
  StructField("maximum_minimum_nights", StringType, true),
  StructField("minimum_maximum_nights", StringType, true),
  StructField("maximum_maximum_nights", StringType, true),
  StructField("minimum_nights_avg_ntm", StringType, true),
  StructField("maximum_nights_avg_ntm", StringType, true),
  StructField("calendar_updated", StringType, true),
  StructField("has_availability", StringType, true),
  StructField("availability_30", StringType, true),
  StructField("availability_60", StringType, true),
  StructField("availability_90", StringType, true),
  StructField("availability_365", StringType, true),
  StructField("calendar_last_scraped", StringType, true),
  StructField("number_of_reviews", StringType, true),
  StructField("number_of_reviews_ltm ", StringType, true),
  StructField("number_of_reviews_l30d", StringType, true),
  StructField("first_review", StringType, true),
  StructField("last_review", StringType, true),
  StructField("review_scores_rating", StringType, true),
  StructField("review_scores_accuracy", StringType, true),
  StructField("review_scores_cleanliness", StringType, true),
  StructField("review_scores_checkin", StringType, true),
  StructField("review_scores_communication", StringType, true),
  StructField("review_scores_location", StringType, true),
  StructField("review_scores_value", StringType, true),
  StructField("license", StringType, true),
  StructField("instant_bookable", StringType, true),
  StructField("calculated_host_listings_count", StringType, true),
  StructField("calculated_host_listings_count_entire_homes", StringType, true),
  StructField("calculated_host_listings_count_private_rooms", StringType, true),
  StructField("calculated_host_listings_count_shared_rooms", StringType, true),
  StructField("reviews_per_month", StringType, true),
))

val listingsDetailed = spark.read.format("csv").option("header", "true").option("sep", ",").option("quote", "\"").option("escape", "\\").schema(listingsDetailedSchema).load("/new-york/listings_detailed_cleaned.csv")

val filteredCalendar = calendarData.filter("listing_id IS NOT NULL").select("listing_id", "calendar_date", "price")

val filteredListingsDetailed = listingsDetailed.filter("id IS NOT NULL").select("id", "neighbourhood_group_cleansed", "room_type")

val etap1Data = filteredCalendar.alias("c").join(filteredListingsDetailed.alias("l"), $"c.listing_id" === $"l.id").select($"c.listing_id", $"c.calendar_date", $"c.price", $"l.neighbourhood_group_cleansed", $"l.room_type")

etap1Data.write.format("hive").mode("overwrite").saveAsTable("etap1_data")

val totalTime = (System.currentTimeMillis - before) / 1000
System.out.println(s"Elapsed (sec): $totalTime")
System.exit(0)
