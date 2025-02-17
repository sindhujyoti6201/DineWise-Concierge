# Scrape Yelp API and store data in DynamoDB
# for cuisine in CUISINES:
#     print(f"Fetching data for {cuisine} cuisine...")
#     for offset in range(0, TOTAL_RESTAURANTS_PER_CUISINE, BATCH_SIZE):
#         restaurants = get_restaurants(cuisine, offset)
#         if not restaurants:
#             break  # Stop if no more results
#         for restaurant in restaurants:
#             insert_restaurant(restaurant)
#         time.sleep(1)  # Prevent hitting API rate limits

# print("✅ Data scraping and storage complete!")