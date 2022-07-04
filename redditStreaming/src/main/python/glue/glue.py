import datetime as dt
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
    base = os.getcwd()
    start = time.time()
    start_datetime = dt.datetime.now()
    subreddit = os.environ["subreddit"]
    aws_client = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret = os.environ["AWS_SECRET_ACCESS_KEY"]
    print(base, start, start_datetime)

    with open("creds.json", "r") as f:
        creds = json.load(f)
        f.close()
        print(creds)
    print("read creds.json.")

    region_name = "us-east-2"
    s3 = boto3.client("s3", region_name=region_name)
    athena = boto3.client("athena", region_name=region_name)
    glue = boto3.client("glue", region_name=region_name)

    get_creds = s3.get_object(Bucket = "reddit-stevenhurwitt", Key = "creds.json")
    print(get_creds)

    data = athena.start_query_execution(
         QueryString = "select * from reddit.{}".format(subreddit),
         ResultConfiguration = {
             'OutputLocation': "s3://reddit-stevenhurwitt/data/{}".format(subreddit)
         })

    print(data)

    glue_job = glue.start_job_run(JobName = "technology_curation", JobRunId = "glue_job_run", JobName = "glue_job")
    print(glue_job)

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

    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    print("created spark & spark context.")

    filepath = "s3a://reddit-stevenhurwitt/technology"
    df = spark.read.format("delta").option("header", "true").load(filepath)
    df.show()
    print("read delta table.")

    

if __name__ == "__main__":
    print("starting glue...")
    glue()
    print("finished glue.")
    sys.exit()