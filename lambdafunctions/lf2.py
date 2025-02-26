import boto3
import json
import random
from opensearchpy import OpenSearch, RequestsHttpConnection

import logging
import os

# Setup logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Services
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
ses = boto3.client('ses')

# Configuration
QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/156041407786/Q1"
DYNAMODB_TABLE = "yelp-restaurants"
USER_SEARCH_HISTORY_TABLE = "UserSearchHistory"
ES_HOST = "search-restaurant-search-5ysoei5mlbqhnt7aiuf3dteflq.aos.us-east-1.on.aws"


username = '_'
password = '_'

# OpenSearch client with basic authentication
es = OpenSearch(
    hosts=[{'host': ES_HOST, 'port': 443}],  # Use the actual ES_HOST here
    http_auth=(username, password),  # Basic Authentication
    use_ssl=True,
    verify_certs=True,
)

def fetch_restaurants_from_es(cuisine):
    """Fetch random restaurant IDs from OpenSearch"""
    logger.info(f"Fetching restaurants for cuisine: {cuisine}")
    query = {
        "query": {
            "match": {
                "Cuisine": cuisine
            }
        }
    }
    res = es.search(index="restaurants", body=query)
    restaurant_ids = [hit["_source"]["RestaurantID"] for hit in res["hits"]["hits"]]
    logger.info(f"Fetched restaurant IDs: {restaurant_ids}")
    return random.sample(restaurant_ids, min(3, len(restaurant_ids)))

def fetch_restaurant_details(restaurant_ids):
    """Fetch restaurant details from DynamoDB"""
    logger.info(f"Fetching details for restaurant IDs: {restaurant_ids}")
    table = dynamodb.Table(DYNAMODB_TABLE)
    restaurant_details = []

    for r_id in restaurant_ids:
        response = table.get_item(Key={'business_id': r_id})
        if 'Item' in response:
            item = response['Item']
            restaurant_details.append({
                "name": item.get("name", "Unknown"),
                "address": item.get("address", "Unknown"),
                "rating": item.get("rating", "N/A")
            })

    logger.info(f"Fetched restaurant details: {restaurant_details}")
    return restaurant_details

def store_recommendation_in_dynamodb(email, location, cuisine):
    """Store the restaurant recommendation in DynamoDB"""
    logger.info(f"Storing recommendation in UserSearchHistory for {email}")

    table = dynamodb.Table(USER_SEARCH_HISTORY_TABLE)

    item = {
        "email": email,
        "Location": location,
        "Cuisine": cuisine
    }

    response = table.put_item(Item=item)
    logger.info(f"Stored recommendation with response: {response}")

    
def send_email(email, location, cuisine, restaurants):
    """Send restaurant recommendations via SES."""
    logger.info(f"Sending email to {email} with {len(restaurants)} restaurant recommendations")

    # Format restaurant list in a table
    column_widths = [45, 45, 10]  # Widths for Name, Address, and Rating
    total_width = sum(column_widths) + 7  # 7 accounts for '|' and spaces

    # Define header and separator
    separator = "-" * total_width

    # Construct the table rows with fixed width
    restaurant_table = "\n\n".join(
        "\n".join([r["name"], r["address"], str(r["rating"])]) for r in restaurants
    )


    # Email subject and body
    email_subject = f"{cuisine} Restaurant Recommendations in {location}"

    email_body = f"""
    Hello,

    Here are some great {cuisine} restaurants in {location} that you might enjoy:


    {separator}
    {restaurant_table}
    {separator}

    Bon appétit!
    """
    print(email_body)
    response = ses.send_email(
        Source="ys6668@nyu.edu",
        Destination={"ToAddresses": [email]},
        Message={
            "Subject": {"Data": email_subject},
            "Body": {"Text": {"Data": email_body}}
        }
    )

    logger.info(f"Email sent with response: {response}")
    return response

def lambda_handler(event, context):
    """Lambda function triggered by CloudWatch to process SQS messages"""
    print("================================")
    print("CALLING LF2")
    print("================================")
    logger.info("Lambda function triggered.")
    
    response = sqs.receive_message(QueueUrl=QUEUE_URL, MaxNumberOfMessages=1)
    
    if 'Messages' not in response:
        logger.warning("No messages in the queue.")
        return {"status": "No messages in queue"}

    message = response['Messages'][0]
    receipt_handle = message['ReceiptHandle']
    body = json.loads(message['Body'])

    print("debugging: ===============================")
    print(body)
    print("debugging: ===============================")

    # Extract user data
    location = body['location']
    cuisine = body['cuisine']
    email = body['email']

    logger.info(f"Processing message: Location={location}, Cuisine={cuisine}, Email={email}")

    # Get restaurant suggestions
    restaurant_ids = fetch_restaurants_from_es(cuisine)
    restaurants = fetch_restaurant_details(restaurant_ids)

    print("===================================================")
    # Store recommendation in DynamoDB
    store_recommendation_in_dynamodb(email, location, cuisine)
    print("===================================================")
    
    # Send recommendations via email
    send_email(email, location, cuisine, restaurants)

    # Delete processed message from SQS
    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)

    logger.info("Message processed successfully and email sent.")
    return {"status": "Email sent successfully!"}