import datetime as dt
from pyspark.sql import SparkSession
from glue_curation_example import glue_curation
from glue_assets import glue_assets
from glue_secrets import glue_secrets
import boto3
import json
import time
import sys
import os
print("imported modules.")

def glue():
    """
    glue

    inputs - None
    outputs - df (spark dataframe)
    """

    # basic glue file
    with open("glue.json", "r") as g:
        glue_args = json.load(g)
        print("glue_args: {}".format(glue_args))
        os.environ["subreddit"] = glue_args["subreddit"]
        g.close()

    # dependency scripts
    glue_assets()
    glue_secrets()

    # basic environment variables
    base = os.getcwd()
    start = time.time()
    start_datetime = dt.datetime.now()
    subreddit = os.environ["subreddit"]
    print(base, start, start_datetime)

    # boto3 secret manager
    aws_client = glue_args["AWS_ACCESS_KEY_ID"]
    aws_secret = glue_args["AWS_SECRET_ACCESS_KEY"]

    secrets = boto3.client("secretmanager", region_name="us-east-2")
    client_id = secrets.get_secret_value(SecretId = aws_client)
    client_secret = secrets.get_secret_value(SecretId = aws_secret)

    # creds file
    with open("creds.json", "r") as f:
        creds = json.load(f)
        f.close()
        print(creds)
    print("read creds.json.")

    # boto3 clients
    region_name = "us-east-2"
    s3 = boto3.client("s3", region_name=region_name)
    athena = boto3.client("athena", region_name=region_name)
    glue = boto3.client("glue", region_name=region_name)

    print(json.dumps(creds))
    get_creds = s3.get_object(Bucket = "reddit-stevenhurwitt", Key = "creds.json")
    s3.put_object(Bucket = "reddit-stevenhurwitt", Key = "creds.json", Body = json.dumps(creds))
    print(get_creds)

    # query athena
    data = athena.start_query_execution(
         QueryString = "select * from reddit.{}".format(subreddit),
         ResultConfiguration = {
             'OutputLocation': "s3://reddit-stevenhurwitt/data/{}".format(subreddit)
         })

    print(data)

    # glue job
    glue_job = glue.start_job_run(JobName = "technology_curation", JobRunId = "glue_job_run", JobName = "glue_job")
    print(glue_job)

    # spark
    spark = SparkSession.builder \
            .appName(subreddit) \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.sql.warehouse.dir", "s3a://reddit-stevenhurwitt/data/warehouse") \
            .config("spark.hadoop.fs.s3a.access.key", creds["AWS_ACCESS_KEY_ID"]) \
            .config("spark.hadoop.fs.s3a.secret.key", creds["AWS_SECRET_ACCESS_KEY"]) \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.consistent.read", "true") \
            .config("spark.hadoop.fs.s3a.fast.upload", "true") \
            .config("spark.hadoop.fs.s3a.server-side-encryption-algorithm", "AES256") \
            .config("spark.hadoop.fs.s3a.server-side-encryption-provider", "org.apache.hadoop.fs.s3a.S3AEncryptionProvider") \
            .config("spark.hadoop.fs.s3a.server-side-encryption-key-provider", "org.apache.hadoop.fs.s3a.DefaultKMSProvider") \
            .config("spark.hadoop.fs.s3a.server-side-encryption-kms-key-id", "alias/aws/s3") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .enableHiveSupport() \
            .getOrCreate()

    # spark context
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print("created spark & spark context.")

    # read filepath delta table
    filepath = "s3a://reddit-stevenhurwitt/technology"
    df = spark.read.format("delta").option("header", "true").load(filepath)
    df.show()
    print("read delta table.")

    # curate data
    print("running glue_curation...")
    glue_curation(glue_args, None)
    print("glue_curation complete.")

    end = time.time()
    end_datetime = dt.datetime.now()
    diff = (end - start)
    diff_datetime = (end_datetime - start_datetime)
    print("took {} seconds to run.".format(diff))
    print("diff datetime: {}".format(diff_datetime))
    sys.exit()
    

if __name__ == "__main__":
    print("starting glue...")
    glue()
    print("finished glue.")
    sys.exit()
