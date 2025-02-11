import requests
import json
import boto3
from datetime import datetime
import time
from decimal import Decimal

API_KEY = 'm5jEBwv5YPhgoEoJOLoWI4GdYF8quKpeGw3dXTi-6sMPCaxQd7E23lKXGUCV4uUXaxKUrTLWr92fW2j5EVLB2MEipizZEUxx9JfuNy1pl5sMiPfeVfN6JsCMCLGqZ3Yx'  # Replace with your API Key
BASE_URL = "https://api.yelp.com/v3/businesses/search"

dynamodb = boto3.resource('dynamodb', 
    region_name='us-east-1')

 # Change to your AWS region
table = dynamodb.Table('yelp-restaurants')

def fetch_yelp_data(cuisine, location, limit=50, offset=0, sort_by='rating', price=None):
    headers = {
        'Authorization': f'Bearer {API_KEY}',
    }
    params = {
        'term': cuisine + " restaurants",
        'location': location,
        'limit': limit,
        'offset': offset,
        'sort_by': sort_by
    }
    if price:
        params['price'] = price
    
    response = requests.get(BASE_URL, headers=headers, params=params)
    return response.json()

# Initialize count as a global variable properly
count = 0

def save_to_dynamodb(restaurants, cuisine=None):
    """Save restaurant data to DynamoDB"""
    global count  # Declare we're using the global count variable
    
    for restaurant in restaurants:
        try:
            item = {
                'business_id': restaurant['id'],
                'name': restaurant['name'],
                'address': ", ".join(restaurant['location']['display_address']),
                'coordinates': {
                    'latitude': Decimal(str(restaurant['coordinates']['latitude'])),
                    'longitude': Decimal(str(restaurant['coordinates']['longitude']))
                },
                'num_reviews': restaurant['review_count'],
                'rating': Decimal(str(restaurant['rating'])),
                'zip_code': restaurant['location'].get('zip_code', 'Unknown'),
                'insertedAtTimestamp': datetime.utcnow().isoformat()
            }
            
            if cuisine:
                item['cuisine'] = cuisine
                
            table.put_item(Item=item)
            count += 1  # Increment count after successful insertion
            
            if count >= 1000:  # Changed to >= to make sure we don't miss the milestone
                print(f"Milestone reached: {count} restaurants inserted!")
            
        except Exception as e:
            print(f"Error inserting {restaurant['name']}: {str(e)}")

def collect_restaurants():
    cuisines = ["Chinese", "Italian", "Japanese", "Mexican", "Indian"]
    all_restaurants = []
    
    # More comprehensive search parameters
    search_params = [
        # East Coast
        {"location": "New York, NY", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Boston, MA", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Miami, FL", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Washington, DC", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Atlanta, GA", "sort_by": "rating", "price": "1,2,3"},
        
        # West Coast
        {"location": "Los Angeles, CA", "sort_by": "rating", "price": "1,2,3"},
        {"location": "San Francisco, CA", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Seattle, WA", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Portland, OR", "sort_by": "rating", "price": "1,2,3"},
        {"location": "San Diego, CA", "sort_by": "rating", "price": "1,2,3"},
        
        # Central
        {"location": "Chicago, IL", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Houston, TX", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Dallas, TX", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Denver, CO", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Minneapolis, MN", "sort_by": "rating", "price": "1,2,3"},
        
        # Additional Major Cities
        {"location": "Las Vegas, NV", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Phoenix, AZ", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Philadelphia, PA", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Austin, TX", "sort_by": "rating", "price": "1,2,3"},
        {"location": "Nashville, TN", "sort_by": "rating", "price": "1,2,3"},
        
        # Additional searches by review count for variety
        {"location": "New York, NY", "sort_by": "review_count", "price": "1,2,3"},
        {"location": "Los Angeles, CA", "sort_by": "review_count", "price": "1,2,3"},
        {"location": "Chicago, IL", "sort_by": "review_count", "price": "1,2,3"},
        {"location": "Houston, TX", "sort_by": "review_count", "price": "1,2,3"},
        {"location": "San Francisco, CA", "sort_by": "review_count", "price": "1,2,3"}
    ]
    
    for cuisine in cuisines:
        cuisine_restaurants = []
        print(f"\nCollecting {cuisine} restaurants...")
        
        for params in search_params:
            if len(cuisine_restaurants) >= 1000:
                break
                
            offset = 0
            max_offset = 150  # Stay well within the 240 limit
            
            while offset < max_offset and len(cuisine_restaurants) < 1000:
                try:
                    data = fetch_yelp_data(
                        cuisine=cuisine,
                        location=params["location"],
                        limit=50,
                        offset=offset,
                        sort_by=params["sort_by"],
                        price=params.get("price")
                    )
                    
                    if 'error' in data:
                        print(f"API Error: {data['error']['description']}")
                        break
                        
                    businesses = data.get("businesses", [])
                    if not businesses:
                        break
                    
                    # Filter out duplicates based on business_id
                    existing_ids = {r['id'] for r in cuisine_restaurants}
                    new_businesses = [b for b in businesses if b['id'] not in existing_ids]
                    
                    if new_businesses:
                        cuisine_restaurants.extend(new_businesses)
                        print(f"Found {len(cuisine_restaurants)} unique {cuisine} restaurants so far...")
                    else:
                        print("No new unique restaurants found in this batch")
                        break  # Skip to next search params if no new restaurants found
                    
                    offset += 50
                    time.sleep(1.5)  # Slightly longer delay to be extra safe
                    
                except Exception as e:
                    print(f"Error fetching data: {str(e)}")
                    time.sleep(2)
                    break
            
            # Add a small delay between different search parameters
            time.sleep(2)
        
        # Trim to exactly 1000 if we got more
        cuisine_restaurants = cuisine_restaurants[:1000]
        all_restaurants.extend(cuisine_restaurants)
        print(f"Finished collecting {len(cuisine_restaurants)} {cuisine} restaurants")
        
        # Save this cuisine's restaurants to DynamoDB
        print(f"Saving {cuisine} restaurants to DynamoDB...")
        save_to_dynamodb(cuisine_restaurants, cuisine)
        
    print(f"\nTotal restaurants collected: {len(all_restaurants)}")
    return all_restaurants

if __name__ == "__main__":
    restaurants = collect_restaurants()