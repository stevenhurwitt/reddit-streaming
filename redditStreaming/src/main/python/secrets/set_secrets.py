import boto3
import json
import time
import sys
import os

def main(my_secret):
    print("main...")
    os.environ["subreddit"] = "AsiansGoneWild"
    os.environ["secret_name"] = str(my_secret)
    os.environ["AWS_ACCESS_KEY_ID"] = "AWS_ACCESS_KEY_ID"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "AWS_SECRET_ACCESS_KEY"
    print("set os environment variables.")
    
    secrets = boto3.client("secretsmanager")
    # my_secret = secrets.get_secret_value(SecretId = "secret_name")
    print("secret client: {}".format(secrets))
    print("my secret: {}".format(os.environ[my_secret]))

if __name__ == "__main__":

    print("starting main.")
    main("AWS_ACCESS_KEY_ID")
    print("ran main.")