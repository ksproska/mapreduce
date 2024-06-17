-- Use the nyc_data database
USE nyc_data;

-- Create an external table for calendar data
CREATE EXTERNAL TABLE IF NOT EXISTS calendar_data (
                                                      listing_id     STRING,
                                                      calendar_date  STRING,
                                                      available      STRING,
                                                      price          STRING,
                                                      adjusted_price STRING,
                                                      minimum_nights STRING,
                                                      maximum_nights STRING
)
    USING csv
    OPTIONS (
        path '/new-york/calendar.csv',
        sep ',',
        quote '\"',
        escape '\\',
        header 'true'
        );

-- Create an external table for detailed listings data
CREATE EXTERNAL TABLE listings_detailed (
                                            id                                           STRING,
                                            listing_url                                  STRING,
                                            scrape_id                                    STRING,
                                            last_scraped                                 STRING,
                                            source                                       STRING,
                                            name                                         STRING,
                                            description                                  STRING,
                                            neighborhood_overview                        STRING,
                                            picture_url                                  STRING,
                                            host_id                                      STRING,
                                            host_url                                     STRING,
                                            host_name                                    STRING,
                                            host_since                                   STRING,
                                            host_location                                STRING,
                                            host_about                                   STRING,
                                            host_response_time                           STRING,
                                            host_response_rate                           STRING,
                                            host_acceptance_rate                         STRING,
                                            host_is_superhost                            STRING,
                                            host_thumbnail_url                           STRING,
                                            host_picture_url                             STRING,
                                            host_neighbourhood                           STRING,
                                            host_listings_count                          STRING,
                                            host_total_listings_count                    STRING,
                                            host_verifications                           STRING,
                                            host_has_profile_pic                         STRING,
                                            host_identity_verified                       STRING,
                                            neighbourhood                                STRING,
                                            neighbourhood_cleansed                       STRING,
                                            neighbourhood_group_cleansed                 STRING,
                                            latitude                                     STRING,
                                            longitude                                    STRING,
                                            property_type                                STRING,
                                            room_type                                    STRING,
                                            accommodates                                 STRING,
                                            bathrooms                                    STRING,
                                            bathrooms_text                               STRING,
                                            bedrooms                                     STRING,
                                            beds                                         STRING,
                                            amenities                                    STRING,
                                            price                                        STRING,
                                            minimum_nights                               STRING,
                                            maximum_nights                               STRING,
                                            minimum_minimum_nights                       STRING,
                                            maximum_minimum_nights                       STRING,
                                            minimum_maximum_nights                       STRING,
                                            maximum_maximum_nights                       STRING,
                                            minimum_nights_avg_ntm                       STRING,
                                            maximum_nights_avg_ntm                       STRING,
                                            calendar_updated                             STRING,
                                            has_availability                             STRING,
                                            availability_30                              STRING,
                                            availability_60                              STRING,
                                            availability_90                              STRING,
                                            availability_365                             STRING,
                                            calendar_last_scraped                        STRING,
                                            number_of_reviews                            STRING,
                                            number_of_reviews_ltm                        STRING,
                                            number_of_reviews_l30d                       STRING,
                                            first_review                                 STRING,
                                            last_review                                  STRING,
                                            review_scores_rating                         STRING,
                                            review_scores_accuracy                       STRING,
                                            review_scores_cleanliness                    STRING,
                                            review_scores_checkin                        STRING,
                                            review_scores_communication                  STRING,
                                            review_scores_location                       STRING,
                                            review_scores_value                          STRING,
                                            license                                      STRING,
                                            instant_bookable                             STRING,
                                            calculated_host_listings_count               STRING,
                                            calculated_host_listings_count_entire_homes  STRING,
                                            calculated_host_listings_count_private_rooms STRING,
                                            calculated_host_listings_count_shared_rooms  STRING,
                                            reviews_per_month                            STRING
)
    USING csv
    OPTIONS (
        path '/new-york/listings_detailed_cleaned.csv',
        sep ',',
        quote '\"',
        escape '\\',
        header 'true'
        );

-- Create views for filtered data
CREATE OR REPLACE VIEW filtered_calendar AS
SELECT listing_id, calendar_date, price
FROM calendar_data
WHERE listing_id IS NOT NULL;

CREATE OR REPLACE VIEW filtered_listings_detailed AS
SELECT id, neighbourhood_group_cleansed, room_type
FROM listings_detailed
WHERE id IS NOT NULL;

-- Join views to create a table
CREATE TABLE etap1_data AS
SELECT c.listing_id,
       c.calendar_date,
       c.price,
       l.neighbourhood_group_cleansed,
       l.room_type
FROM filtered_calendar c
         JOIN filtered_listings_detailed l ON c.listing_id = l.id;
