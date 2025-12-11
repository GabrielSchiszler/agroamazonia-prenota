import os
import json
import boto3

secrets_client = boto3.client('secretsmanager')
SECRET_ARN = os.environ['SECRET_ARN']

# Cache in Lambda memory
api_keys_cache = None

def get_api_keys():
    global api_keys_cache
    if api_keys_cache is None:
        response = secrets_client.get_secret_value(SecretId=SECRET_ARN)
        api_keys_cache = json.loads(response['SecretString'])
    return api_keys_cache

def handler(event, context):
    token = event['authorizationToken']
    
    try:
        api_keys = get_api_keys()
        
        if token not in api_keys:
            raise Exception('Unauthorized')
        
        client_info = api_keys[token]
        
        if client_info.get('status') != 'active':
            raise Exception('Unauthorized')
        
        return {
            'principalId': client_info.get('client_name', 'unknown'),
            'policyDocument': {
                'Version': '2012-10-17',
                'Statement': [{
                    'Action': 'execute-api:Invoke',
                    'Effect': 'Allow',
                    'Resource': event['methodArn'].split('/')[0] + '/*'
                }]
            },
            'context': {
                'clientName': client_info.get('client_name', 'unknown')
            }
        }
    
    except Exception as e:
        print(f"Authorization failed: {str(e)}")
        raise Exception('Unauthorized')
