data_calendar = LOAD '/new-york/calendar.csv' USING PigStorage(',') AS (
    listing_id: chararray, date: chararray, available: chararray, price: chararray, adjusted_price: chararray,
    minimum_nights: chararray, maximum_nights: chararray
);
data_calendar = FOREACH data_calendar GENERATE listing_id, date, price;
data_calendar = FILTER data_calendar BY listing_id IS NOT NULL;

DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader();

data_listing_detailed = LOAD '/new-york/listings_detailed_cleaned.csv' USING CSVLoader(',') AS (
    id: chararray, listing_url: chararray, scrape_id: chararray, last_scraped: chararray, source: chararray,
    name: chararray, description: chararray, neighborhood_overview: chararray, picture_url: chararray, host_id: chararray,
    host_url: chararray, host_name: chararray, host_since: chararray, host_location: chararray, host_about: chararray,
    host_response_time: chararray, host_response_rate: chararray, host_acceptance_rate: chararray, host_is_superhost: chararray,
    host_thumbnail_url: chararray, host_picture_url: chararray, host_neighbourhood: chararray, host_listings_count: chararray,
    host_total_listings_count: chararray, host_verifications: chararray, host_has_profile_pic: chararray,
    host_identity_verified: chararray, neighbourhood: chararray, neighbourhood_cleansed: chararray,
    neighbourhood_group_cleansed: chararray, latitude: chararray, longitude: chararray, property_type: chararray,
    room_type: chararray, accommodates: chararray, bathrooms: chararray, bathrooms_text: chararray, bedrooms: chararray,
    beds: chararray, amenities: chararray, price: chararray, minimum_nights: chararray, maximum_nights: chararray,
    minimum_minimum_nights: chararray, maximum_minimum_nights: chararray, minimum_maximum_nights: chararray,
    maximum_maximum_nights: chararray, minimum_nights_avg_ntm: chararray, maximum_nights_avg_ntm: chararray,
    calendar_updated: chararray, has_availability: chararray, availability_30: chararray, availability_60: chararray,
    availability_90: chararray, availability_365: chararray, calendar_last_scraped: chararray, number_of_reviews: chararray,
    number_of_reviews_ltm: chararray, number_of_reviews_l30d: chararray, first_review: chararray, last_review: chararray,
    review_scores_rating: chararray, review_scores_accuracy: chararray, review_scores_cleanliness: chararray,
    review_scores_checkin: chararray, review_scores_communication: chararray, review_scores_location: chararray,
    review_scores_value: chararray, license: chararray, instant_bookable: chararray, calculated_host_listings_count: chararray,
    calculated_host_listings_count_entire_homes: chararray, calculated_host_listings_count_private_rooms: chararray,
    calculated_host_listings_count_shared_rooms: chararray, reviews_per_month: chararray
);

data_listing_detailed = FOREACH data_listing_detailed GENERATE id, neighbourhood_group_cleansed, room_type;
data_listing_detailed = FILTER data_listing_detailed BY id IS NOT NULL;

merged_data = JOIN data_calendar BY listing_id, data_listing_detailed BY id;
merged_data = FOREACH merged_data GENERATE data_calendar::listing_id, data_calendar::date, data_calendar::price,
    data_listing_detailed::neighbourhood_group_cleansed, data_listing_detailed::room_type;

merged_data_head = LIMIT merged_data 10;
DUMP merged_data_head;

RMF /new-york/pig/etap1.csv ;
STORE merged_data INTO '/new-york/pig/etap1.csv' USING PigStorage(',');
