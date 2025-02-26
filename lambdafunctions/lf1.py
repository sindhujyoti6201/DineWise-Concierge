import json
import boto3
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

sqs_client = boto3.client('sqs')
lambda_client = boto3.client('lambda')  # Lambda client for invoking Lambda 3

SQS_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/156041407786/Q1'
LAMBDA_3 = "LF3"

def lambda_handler(event, context):
    """
    Handles events from Amazon Lex.
    """
    print("================================")
    print("CALLING LF1")
    print("================================")
    

    logger.info(f"Received event: {json.dumps(event)}") # Log the entire event for debugging

    intent_name = event['sessionState']['intent']['name']

    # --- Intent Handling ---

    if intent_name == 'GreetingIntent':
        return handle_greeting_intent()
    elif intent_name == 'ThankYouIntent':
        return handle_thank_you_intent()
    elif intent_name == 'DiningSuggestionsIntent':
        return handle_dining_suggestions_intent(event)
    elif intent_name == 'FetchPreviousSearchIntent':
        print("REACHED INSIDE")
        return handle_fetch_previous_search_intent(event)
    else:
        # Fallback for unhandled intents
        return handle_unrecognized_intent()

# --- Intent Handlers ---
def handle_fetch_previous_search_intent(event):
    """
    Handles FetchPreviousSearchIntent by calling Lambda 3 to fetch previous searches for the given email.
    """
    slots = event['sessionState']['intent']['slots']
    email = get_slot_value(slots, 'Email')

    if not email:
        logger.info("Email slot is missing. Delegating back to Lex.")
        return {
            'sessionState': {
                'dialogAction': {'type': 'Delegate'},
                'intent': event['sessionState']['intent']
            }
        }

    # Invoke Lambda 3 to fetch previous searches
    lambda_payload = {'email': email}
    response = lambda_client.invoke(
        FunctionName=LAMBDA_3,
        InvocationType='RequestResponse',  # Waits for a response
        Payload=json.dumps(lambda_payload)
    )


    lambda_response_payload = json.loads(response['Payload'].read().decode('utf-8'))
    logger.info(f"Lambda 3 response: {lambda_response_payload}")

    # Extract search history from response
    status_code = lambda_response_payload.get('statusCode')
    body = lambda_response_payload.get('body')
    email = body.get('email')
    cuisine = body.get('cuisine')
    location = body.get('location')
    if status_code == 200 and email and cuisine and location:
        message = f"Your previous search was for {cuisine} cuisine in {location}. Expect my suggestions shortly! Have a good day."
    else:
        message = "I couldn't find any previous restaurant searches for you."
        


    # Return the search history to the user
    response = {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'FetchPreviousSearchIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText', 
                'content': message
            }
        ]
    }

    logger.info(f"FetchPreviousSearchIntent response: {json.dumps(response)}")
    return response


def handle_greeting_intent():
    """
    Handles the GreetingIntent.
    """
    response = {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'GreetingIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': 'Hi there, how can I help you find a restaurant today?'
            }
        ]
    }
    logger.info(f"GreetingIntent response: {json.dumps(response)}")
    return response

def handle_thank_you_intent():
    """
    Handles the ThankYouIntent.
    """
    response = {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': 'ThankYouIntent',
                'state': 'Fulfilled'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': "You're welcome!  I hope you have a wonderful meal."
            }
        ]
    }
    logger.info(f"ThankYouIntent response: {json.dumps(response)}")
    return response

def handle_dining_suggestions_intent(event):
    """
    Handles the DiningSuggestionsIntent, collecting slot data,
    sending to SQS, and confirming with the user.
    """
    slots = event['sessionState']['intent']['slots']

    # Extract slot values, handling potential missing slots gracefully
    location = get_slot_value(slots, 'Location')
    cuisine = get_slot_value(slots, 'Cuisine')
    dining_time = get_slot_value(slots, 'DiningTime')
    number_of_people = get_slot_value(slots, 'NumberOfPeople')
    email = get_slot_value(slots, 'Email')

    logger.info(f"Slot values: location={location}, cuisine={cuisine}, dining_time={dining_time}, number_of_people={number_of_people}, email={email}")

    # Check if all required slots are filled
    if all([location, cuisine, dining_time, number_of_people, email]):
        # Send data to SQS
        try:
            send_to_sqs(location, cuisine, dining_time, number_of_people, email)
            logger.info("Successfully sent message to SQS")

            # Confirm with the user
            response = {
                'sessionState': {
                    'dialogAction': {
                        'type': 'Close'
                    },
                    'intent': {
                        'name': 'DiningSuggestionsIntent',
                        'state': 'Fulfilled'
                    }
                },
                'messages': [
                    {
                        'contentType': 'PlainText',
                        'content': "You’re all set. Expect my suggestions shortly! Have a good day."
                    }
                ]
            }
            logger.info(f"DiningSuggestionsIntent (fulfilled) response: {json.dumps(response)}")
            return response

        except Exception as e:
            logger.error(f"Error sending to SQS: {e}")
            # Handle SQS error (e.g., inform the user, retry)
            response = {
                'sessionState': {
                    'dialogAction': {
                        'type': 'Close'
                    },
                    'intent': {
                        'name': 'DiningSuggestionsIntent',
                        'state': 'Failed'
                    }
                },
               'messages': [
                    {
                        'contentType': 'PlainText',
                        'content': "Sorry, there was an error processing your request. Please try again later."
                    }
                ]
            }
            return response

    else:
        # Delegate back to Lex to elicit missing slots
        logger.info("Delegating back to Lex to elicit missing slots")
        return {
            'sessionState': {
                'dialogAction': {
                    'type': 'Delegate'
                },
                'intent': event['sessionState']['intent']  # Keep the current intent
            }
        }

def handle_unrecognized_intent():
    """
    Handles intents that are not recognized.
    """
    response = {
        'sessionState': {
            'dialogAction': {
                'type': 'Close'
            },
            'intent': {
                'name': None,
                'state': 'Failed'
            }
        },
        'messages': [
            {
                'contentType': 'PlainText',
                'content': "I'm sorry, I didn't understand. How can I help you find a restaurant?"
            }
        ]
    }
    return response

# --- Helper Functions ---

def get_slot_value(slots, slot_name):
    """
    Safely retrieves the value of a slot from the slots dictionary.
    Returns None if the slot or its value is missing.
    """
    try:
        return slots[slot_name]['value']['interpretedValue']
    except (KeyError, TypeError):
        return None

def send_to_sqs(location, cuisine, dining_time, number_of_people, email):
    """
    Sends the dining suggestion request to the SQS queue.
    """
    message_body = {
        'location': location,
        'cuisine': cuisine,
        'dining_time': dining_time,
        'number_of_people': number_of_people,
        'email': email
    }

    try:
        response = sqs_client.send_message(
            QueueUrl=SQS_QUEUE_URL,
            MessageBody=json.dumps(message_body)
        )
        logger.info(f"SQS Send Message response: {response}")  # Log the SQS response

    except Exception as e:
        logger.error(f"Error sending message to SQS: {e}")
        raise # Re-raise the exception so the calling function knows it failed