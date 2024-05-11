-- calendar
data_calendar = LOAD '/new-york/calendar.csv' USING PigStorage(',') AS (
listing_id: chararray, date: chararray, available: chararray, price: chararray, adjusted_price: chararray, minimum_nights: chararray, maximum_nights: chararray
);
DESCRIBE data_calendar;
data_calendar = FOREACH data_calendar GENERATE listing_id, date, price;
data_calendar_head = LIMIT data_calendar 10;
DUMP data_calendar_head;

-- listing_detailed
data_listing_detailed = LOAD '/new-york/listings_detailed_cleaned.csv' USING PigStorage(',') AS (
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
DESCRIBE data_listing_detailed;
data_listing_detailedr = FOREACH data_listing_detailed GENERATE id, neighbourhood_group_cleansed, room_type;
data_listing_detailed_head = LIMIT data_listing_detailed 10;
DUMP data_listing_detailed_head;

-- NYC events
data_events = LOAD '/new-york/NYC_events_filtered.csv' USING PigStorage(',') AS (
    event_id: chararray, EventName: chararray, start_date: chararray, EndDate_Time: chararray, EventAgency: chararray,
    EventType: chararray, event_borough: chararray, EventLocation: chararray, EventStreetSide: chararray,
    StreetClosureType: chararray, CommunityBoard: chararray, PolicePrecinct: chararray
);
DESCRIBE data_events;
data_events = FOREACH data_events GENERATE event_id, start_date, event_borough;
data_events_head = LIMIT data_events 10;
DUMP data_events_head;

-- weather data
data_weather = LOAD '/new-york/weather_data.csv' USING PigStorage(',') AS (date: chararray, temperature: double, weather_conditions: chararray);
DESCRIBE data_weather;
data_weather_head = LIMIT data_weather 10;
DUMP data_weather_head;

-- summary
DESCRIBE data_calendar;
DUMP data_calendar_head;
DESCRIBE data_listing_detailed;
DUMP data_listing_detailed_head;
DESCRIBE data_listing_detailed;
DUMP data_listing_detailed_head;
DESCRIBE data_events;
DUMP data_events_head;
data_weather = LOAD '/new-york/weather_data.csv' USING PigStorage(',') AS (date: chararray, temperature: double, weather_conditions: chararray);
DUMP data_weather_head;
