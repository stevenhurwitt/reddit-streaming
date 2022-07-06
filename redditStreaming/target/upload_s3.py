import pprint
import boto3
import json
import time
import sys
import os

def main():
    # init main function
    pp = pprint.PrettyPrinter(indent = 1)
    base = os.getcwd()
    start = time.time()
    print("initialized main function.")

    # set bucket/subreddit
    bucket = "reddit-stevenhurwitt"
    subreddit = "AsiansGoneWild"
    target = "AsiansGoneWild_clean"

    # start s3
    s3 = boto3.client("s3", region_name = "us-east-2")
    print("s3: {}".format(s3))
    print("s3 client commands:")
    pp.pprint(dir(s3))
    # s3.get_secret_value(SecretId = subreddit)

    # start secret manager
    secrets = boto3.client("secretsmanager", region_name = "us-east-2")
    print("secret manager: {}".format(secrets))
    print("secrets manager: ")
    pp.pprint(dir(secrets))

    # get secret values
    # my_subreddit = secrets.get_secret_value(SecretId = "subreddit")
    my_subreddit = os.environ["subreddit"]
    my_client_id = "AWS_ACCESS_KEY_ID"
    my_secret_id = "AWS_SECRET_ACESS_KEY"

    my_client = secrets.get_secret_value(SecretId = my_client_id)
    my_secret = secrets.get_secret_value(SecretId = my_secret_id)
    aws_client = my_client["SecretString"]
    aws_secret = my_secret["SecretString"]

    os.environ["AWS_ACCESS_KEY_ID"] = aws_client
    os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret
    print("subreddit: {}".format(subreddit))
    print("aws_client: {}".format(aws_client))
    print("secret key length (n) = {}.".format(len(aws_secret)))

    # get s3 buckets
    my_buckets = s3.list_buckets()
    pp.pprint(my_buckets)

    # more...
    print(dir(s3))


if __name__ == "__main__":
    print("starting main...")
    main()
    print("main finished.")
    sys.exit()