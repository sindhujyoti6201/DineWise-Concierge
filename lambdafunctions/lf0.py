import json
import boto3
import uuid
import time

# Initialize Lex client
lex_client = boto3.client('lexv2-runtime')

BOT_ID = "V6GL43HGXU"
BOT_ALIAS_ID = "V2OIBDKJ1Y"
LOCALE_ID = "en_US"  # Update if using a different language

def lambda_handler(event, context):
    """ We are getting the input from the frontend in the form: 'body': '{"messages":[{"type":"unstructured","unstructured":{"text":"Hi"}}]}' """
    print("================================")
    print("CALLING LF0")
    print("================================")

    print("=================================")
    print(event)
    print("=================================")
    try:
        # Parse the JSON body (string → dictionary)
        body = json.loads(event["body"])

        # Extract user message
        messages = body.get("messages", [])

        if messages and isinstance(messages, list) and "unstructured" in messages[0]:
            user_message = messages[0]["unstructured"].get("text", "")
        else:
            user_message = ""

        if not user_message:
            return {
                "statusCode": 400,
                "headers": {
                    'Access-Control-Allow-Origin': '*',  # Change '*' to your domain if needed
                    'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
                    'Access-Control-Allow-Headers': 'Content-Type, Authorization, sessionId'
                },
                "body": json.dumps({
                    "code": 400,
                    "message": "No message provided"
                })
            }

        # Extract session ID from headers
        session_id = messages[0]["unstructured"].get("id", "")
        print("Session ID:", session_id)

        # Call Amazon Lex
        lex_response = lex_client.recognize_text(
            botId=BOT_ID,
            botAliasId=BOT_ALIAS_ID,
            localeId=LOCALE_ID,
            sessionId=session_id,
            text=user_message
        )

        print("Lex Response:", lex_response)
        
        # Extract Lex's response message
        lex_message = lex_response.get("messages", [{}])[0].get("content", "I'm sorry, I didn't understand that.")

        # Format response for frontend
        formatted_response = {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "id": session_id,  # Include session ID in the response
                        "text": lex_message,  # Lex's response
                        "timestamp": str(int(time.time()))  # Current timestamp
                    }
                }
            ]
        }

        # Return response to API Gateway (HTTP 200)
        return {
            "statusCode": 200,
            "headers": {
                'Access-Control-Allow-Origin': '*',  # Change '*' to your domain if needed
                'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization, sessionId'
            },
            "body": json.dumps(formatted_response)
        }
    
    except lex_client.exceptions.AccessDeniedException:
        # Handle 403 error
        print("Access Denied: The Lambda function does not have permissions to call Lex.")
        return {
            "statusCode": 403,
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization, sessionId'
            },
            "body": json.dumps({
                "code": 403,
                "message": "Access Denied: Unauthorized to call Lex."
            })
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'POST, GET, OPTIONS',
                'Access-Control-Allow-Headers': 'Content-Type, Authorization, sessionId'
            },
            "body": json.dumps({
                "code": 500,
                "message": str(e)
            })
        }
