import boto3
import json

# Define valid users in a dictionary
def load_dict_from_json(file_path):
    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)
    return data

secrets_file = "../secrets.json"

SECRETS = load_dict_from_json(secrets_file)

# AWS Credentials (ensure these are handled securely, e.g., via environment variables or IAM roles in production)
AWS_ACCESS_KEY_ID = SECRETS["aws_access_key_id"]
AWS_SECRET_ACCESS_KEY = SECRETS["aws_secret_access_key"]
REGION_NAME = SECRETS["REGION"]
INFERENCE_PROFILE_ARN = SECRETS["INFERENCE_PROFILE_ARN"]

# Initialize Bedrock Runtime client
bedrock_runtime = boto3.client(
    service_name='bedrock-runtime',
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Initialize Textract client
textract_client = boto3.client(
    service_name='textract',
    region_name=REGION_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def call_claude_api(prompt_text):
    """Calls the Claude API via Bedrock Runtime."""
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4096,
        "messages": [
            {
                "role": "user",
                "content": prompt_text
            }
        ]
    }

    try:
        response = bedrock_runtime.invoke_model(
            modelId=INFERENCE_PROFILE_ARN,
            contentType='application/json',
            accept='application/json',
            body=json.dumps(payload)
        )
        response_body = json.loads(response['body'].read())
        return response_body['content'][0]['text'].strip()
    except Exception as e:
        print(f"Error calling Claude API: {e}")
        return None

# Example usage (optional - for testing, can be removed or commented out)
if __name__ == "__main__":
    print("AWS SDK clients initialized.")
    # Test Textract (requires an image or document)
    # print(f"Textract client: {textract_client}")
    
    # Test Bedrock Claude API
    # test_prompt = "Hello, Claude. How are you today?"
    # claude_response = call_claude_api(test_prompt)
    # if claude_response:
    #     print(f"Claude API Response: {claude_response}")
    # else:
    #     print("Failed to get response from Claude API.")

