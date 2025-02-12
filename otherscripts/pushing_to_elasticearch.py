import boto3
from requests_aws4auth import AWS4Auth
import requests
import json
from decimal import Decimal

# Helper class to convert Decimal to float for JSON serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

# Update these values to match your OpenSearch domain
host = 'search-restaurants-domain-mhjjqfrnb4ocw32pyezt2zhjli.aos.eu-north-1.on.aws'  # Your OpenSearch domain endpoint
region = 'eu-north-1'  # Changed to match the OpenSearch domain region
service = 'es'  # Changed back to 'es' for regular OpenSearch

# Get AWS credentials
credentials = boto3.Session(region_name='eu-north-1').get_credentials()  # Added region to session
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    region,
    service,
    session_token=credentials.token
)

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

# Scan the table to get all restaurant data
response = table.scan()
restaurants = response['Items']
print(f"Extracted {len(restaurants)} restaurants from DynamoDB")

# Prepare bulk data
bulk_data = ""
for restaurant in restaurants:
    # Index action
    action = {"index": {"_index": "restaurants", "_id": restaurant["business_id"]}}
    # Document data
    document = {
        "RestaurantID": restaurant["business_id"],
        "Cuisine": restaurant["cuisine"],
        "Name": restaurant.get("name", ""),
        "Address": restaurant.get("address", ""),
        "Rating": float(restaurant.get("rating", 0)),
        "ReviewCount": restaurant.get("num_reviews", 0)
    }
    
    bulk_data += json.dumps(action) + "\n"
    bulk_data += json.dumps(document, cls=DecimalEncoder) + "\n"

print("Formatted data for OpenSearch bulk upload")

# OpenSearch endpoint
url = f'https://{host}/restaurants/_bulk'
headers = {'Content-Type': 'application/json'}

# Send bulk data to OpenSearch
try:
    response = requests.post(url, auth=awsauth, headers=headers, data=bulk_data.encode('utf-8'))
    
    if response.status_code == 200:
        print("Successfully uploaded data to OpenSearch")
        print(f"Response: {response.json()}")
    else:
        print(f"Error uploading to OpenSearch. Status code: {response.status_code}")
        print(f"Error message: {response.text}")
except Exception as e:
    print(f"Exception occurred: {str(e)}")