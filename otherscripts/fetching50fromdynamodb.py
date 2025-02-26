import boto3

# Initialize AWS resources
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

# Elasticsearch client
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch(
    ["https://search-restaurant-search-5ysoei5mlbqhnt7aiuf3dteflq.us-east-1.es.amazonaws.com"],
    request_timeout=30,
    max_retries=10,
    retry_on_timeout=True
)


# DynamoDB: Scan table and fetch only RestaurantID and Cuisine
def fetch_dynamodb_data(cuisine, limit=50):
    # Initialize the list to store restaurant data
    restaurants = []
    
    # Start scanning with no LastEvaluatedKey
    last_evaluated_key = None
    
    while len(restaurants) < limit:
        # Perform the scan request with the filter and pagination
        scan_params = {
            'FilterExpression': "cuisine = :cuisine",
            'ExpressionAttributeValues': {":cuisine": cuisine},
            'Limit': limit - len(restaurants)  # Fetch remaining records to reach limit
        }
        
        if last_evaluated_key:
            scan_params['ExclusiveStartKey'] = last_evaluated_key
        
        # Scan the table with the parameters
        response = table.scan(**scan_params)

        # Extract the restaurant data from the response
        for item in response.get("Items", []):
            restaurant_id = item.get("business_id")
            cuisine_type = item.get("cuisine")

            if restaurant_id and cuisine_type:
                restaurants.append({
                    "RestaurantID": restaurant_id,
                    "Cuisine": cuisine_type
                })
        
        # Check if there are more records to fetch
        last_evaluated_key = response.get("LastEvaluatedKey")
        
        # If there are no more records, break the loop
        if not last_evaluated_key:
            break

    return restaurants


# Elasticsearch: Insert data into existing index
index_name = "restaurants"


# Fetch and insert data for three cuisines: Chinese, Indian, and Italian
def fetch_and_insert():
    cuisines = ["Chinese", "Indian", "Italian"]
    total_inserted = 0
    max_entries_per_cuisine = 50

    for cuisine in cuisines:
        print(f"Processing cuisine: {cuisine}")
        restaurant_data = fetch_dynamodb_data(cuisine, limit=max_entries_per_cuisine)
        print(restaurant_data)
    #     if restaurant_data:
    #         insert_to_elasticsearch(restaurant_data)
    #         total_inserted += len(restaurant_data)
    #         print(f"Inserted {len(restaurant_data)} restaurants for {cuisine}.")
    #     else:
    #         print(f"No data found for cuisine: {cuisine}")

    # print(f"Total of {total_inserted} restaurants inserted into Elasticsearch.")


# Call the function to fetch and insert data
fetch_and_insert()