# from botocore.exceptions import ClientError
import datetime as dt
import pprint
import base64
import boto3
import json
import time
import sys
import os


# metadata
pp = pprint.PrettyPrinter(indent = 1)
start_time = time.time()
start_datetime = dt.datetime.now()
base = os.getcwd()
region_name = "us-east-2"
print("set metadata.")

# aws clients
s3 = boto3.client("s3", region_name=region_name)
athena = boto3.client("athena", region_name = region_name)
print("created aws clients.")

# os environment variables
subreddit = os.environ["subreddit"]
secret_name = os.environ["secret_name"]
aws_client = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
print("read os environment variables.")

# Create a Secrets Manager client
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)
print("initialized secrets manager client.")

# In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
# See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
# We rethrow the exception by default.

try:
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
except Exception as e:
    if e.response['Error']['Code'] == 'DecryptionFailureException':
        # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
        # Deal with the exception here, and/or rethrow at your discretion.
        raise e
    elif e.response['Error']['Code'] == 'InternalServiceErrorException':
        # An error occurred on the server side.
        # Deal with the exception here, and/or rethrow at your discretion.
        raise e
    elif e.response['Error']['Code'] == 'InvalidParameterException':
        # You provided an invalid value for a parameter.
        # Deal with the exception here, and/or rethrow at your discretion.
        raise e
    elif e.response['Error']['Code'] == 'InvalidRequestException':
        # You provided a parameter value that is not valid for the current state of the resource.
        # Deal with the exception here, and/or rethrow at your discretion.
        raise e
    elif e.response['Error']['Code'] == 'ResourceNotFoundException':
        # We can't find the resource that you asked for.
        # Deal with the exception here, and/or rethrow at your discretion.
        raise e
    # print(e)

else:
    # Decrypts secret using the associated KMS key.
    # Depending on whether the secret is a string or binary, one of these fields will be populated.
    if 'SecretString' in get_secret_value_response:
        secret = get_secret_value_response['SecretString']
    else:
        decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        
# Your code goes here.
def main():

    print("starting glue.")
    print("s3: {}".format(s3))
    print("athena: {}".format(athena))
    print("subreddit: {}".format(subreddit))
    print("secret_name: {}".format(secret_name))

    sys.exit()