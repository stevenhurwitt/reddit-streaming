import boto3
import json
import os

if __name__ == "__main__""
    s3 = boto3.client("s3")
    athena = boto3.client("athena")
    glue = boto3.client("glue")
    ec2 = boto3.client("ec2")
    rds = boto3.client("rds")
    print("initialized boto3 clients.")