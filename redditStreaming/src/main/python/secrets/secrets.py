from botocore.exceptions import ClientError
import datetime as dt
import reddit
import pprint
import base64
import boto3
import glue
import json
import time
import sys
import os

# Your code goes here.
def main():

    print("starting glue.")
    reddit.reddit_streaming()
    sys.exit()

def init():

    s3 = boto3.client("s3")
    athena = boto3.client("athena")
    secrets = boto3.client("secretmanager")
    subreddit = os.environ["subreddit"]
    secret_name = os.environ["secret_name"]
    print("secret name: {}".format(secret_name))
    print("subreddit: {}".format(subreddit))
    print("secrets: {}".format(secrets))
    print("s3: {}".format(s3))
    print("athena: {}".format(athena))
    print("init complete.")

if __name__ == "__main__":

    init()

    main()

    secret_name = os.environ["secret_name"]
    subreddit = os.environ["subreddit"]
    s3 = boto3.client("s3")
    athena = boto3.client("athena")
    # secrets = boto3.client("secretsmanager")

    try:
        get_secret_value_response = secrets.get_secret_value(
            SecretId=secret_name
        )

    except Exception as e:
        print(e)
        pass

    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])