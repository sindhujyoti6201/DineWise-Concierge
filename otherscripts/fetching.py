import boto3

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Table name
table = dynamodb.Table('yelp-restaurants')

# Function to fetch data by Cuisine
def fetch_data(cuisine, limit=50):
    response = table.scan(
        FilterExpression="cuisine = :cuisine",
        ExpressionAttributeValues={":cuisine": cuisine},
        Limit=limit
    )
    print(response)
    
    return response['Items']

# Example usage
cuisine = 'Chinese'
data = fetch_data(cuisine)
for item in data:
    print(item)