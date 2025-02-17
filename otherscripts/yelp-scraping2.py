import requests
import json
import boto3
from datetime import datetime
import time
from decimal import Decimal

API_KEY = 'm5jEBwv5YPhgoEoJOLoWI4GdYF8quKpeGw3dXTi-6sMPCaxQd7E23lKXGUCV4uUXaxKUrTLWr92fW2j5EVLB2MEipizZEUxx9JfuNy1pl5sMiPfeVfN6JsCMCLGqZ3Yx'  # Replace with your API Key

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Yelp API Configuration
YELP_ENDPOINT = "https://api.yelp.com/v3/businesses/search"
HEADERS = {"Authorization": f"Bearer {API_KEY}"}

# AWS DynamoDB Configuration
AWS_REGION = "us-east-1"
DYNAMODB_TABLE = "yelp-restaurants"

# Initialize DynamoDB resource
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

# Define cuisines to scrape
CUISINES = ["Chinese", "Italian", "Mexican", "Indian", "Japanese"]
TOTAL_RESTAURANTS_PER_CUISINE = 1000  # Yelp allows max 1000 per cuisine
BATCH_SIZE = 50  # Yelp allows max 50 results per request


def delete_all_items():
    """Deletes all items in the DynamoDB table."""
    try:
        # Scan the table to get all the keys (primary key)
        response = table.scan()
        items = response.get('Items', [])
        
        # Delete each item by its primary key (business_id)
        for item in items:
            business_id = item["business_id"]
            table.delete_item(Key={"business_id": business_id})
            print(f"Deleted: {business_id}")
        
        # Handle pagination if there are more items
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items = response.get('Items', [])
            for item in items:
                business_id = item["business_id"]
                table.delete_item(Key={"business_id": business_id})
                print(f"Deleted: {business_id}")
                
        print("✅ All items deleted successfully.")
    except Exception as e:
        print(f"Error deleting items: {str(e)}")


def insert_restaurant(restaurant):
    try:
        item = {
            "business_id": restaurant["id"],
            "name": restaurant.get("name", "Unknown"),
            "address": restaurant["location"].get("address1", "N/A"),
            "coordinates": {
                "latitude": Decimal(str(restaurant["coordinates"]["latitude"])),
                "longitude": Decimal(str(restaurant["coordinates"]["longitude"]))
            },
            "num_reviews": Decimal(restaurant.get("review_count", 0)),
            "rating": Decimal(str(restaurant.get("rating", 0.0))),
            "zip_code": restaurant["location"].get("zip_code", "N/A"),
            "insertedAtTimestamp": datetime.utcnow().isoformat()
        }
        table.put_item(Item=item)
        # print(f"Inserted: {item['name']} ({item['business_id']})")
    except Exception as e:
        print(f"Error inserting data: {str(e)}")


def get_restaurants(cuisine, offset=0, location="US"):
    """Fetch restaurants from Yelp API for a specific cuisine with pagination."""
    results_left = TOTAL_RESTAURANTS_PER_CUISINE - offset
    limit = min(50, results_left)  # Limit can be at most 50 per request
    
    if limit == 0:
        return []

    params = {
        "location": location,
        "term": f"{cuisine} restaurants",
        "limit": limit,
        "offset": offset
    }
    
    response = requests.get(YELP_ENDPOINT, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json().get("businesses", [])
    else:
        print(f"Error: {response.json()}")
        return []


def fetch_all_restaurants(cuisine):
    cities = [
        "New York", "Los Angeles", "San Francisco", "Chicago", "Houston", "Dallas", "Seattle",
        "Miami", "Boston", "Austin", "Phoenix", "Denver", "Washington, D.C.", "Las Vegas", "San Diego", 
        "Atlanta", "Orlando", "Detroit", "Minneapolis", "Philadelphia", "Portland", "Charlotte", 
        "Indianapolis", "Nashville", "Kansas City", "Cleveland", "Salt Lake City", "Columbus", "Raleigh", 
        "Tampa", "Louisville", "Sacramento", "Omaha", "Pittsburgh", "Baltimore", "St. Louis", 
        "Cincinnati", "Madison", "Buffalo", "Milwaukee", "New Orleans", "Richmond", "Hartford", 
        "Birmingham", "Tucson", "Boise", "Anchorage", "Des Moines", "Fresno", "Chattanooga", 
        "Lubbock", "Wichita", "Laredo", "Hialeah", "Tucson", "Bakersfield", "Cedar Rapids"
    ]
    
    total_inserted = 0
    max_restaurants_per_cuisine = 1000  # Limit to 1000 restaurants per cuisine

    for location in cities:
        if total_inserted >= max_restaurants_per_cuisine:
            break  # Stop if 1000 restaurants have been fetched

        print(f"\nFetching data for {cuisine} cuisine in {location}...")
        
        for offset in range(0, max_restaurants_per_cuisine - total_inserted, BATCH_SIZE):
            restaurants = get_restaurants(cuisine, offset, location)
            if not restaurants:
                break  # Stop if no more results
            
            for restaurant in restaurants:
                insert_restaurant(restaurant)
                total_inserted += 1
                if total_inserted >= max_restaurants_per_cuisine:
                    break  # Stop once we reach the desired limit
            
            print(f"Fetched and inserted {total_inserted} restaurants for {cuisine} in {location}...")
            time.sleep(1)  # Prevent hitting API rate limits

    print(f"✅ Finished inserting {total_inserted} restaurants for {cuisine}.")

# Call this function before inserting new data
delete_all_items()
CUISINES = ["Chinese", "Italian", "Mexican", "Indian", "Japanese"]
# Scrape Yelp API and store data in DynamoDB for each cuisine
for cuisine in CUISINES:
    fetch_all_restaurants(cuisine)
