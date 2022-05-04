from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime as dt
import requests
import pprint
import json
import sys
import os


# base = os.path.join("home", "ubuntu", "reddit-streaming")
base = os.getcwd()
creds_path = os.path.join(base, "creds.json")
# creds_path = os.path.join(base, "creds.json")
# db_creds_path = os.path.join(base, "db_creds.json")
# db_crfeds_path = os.path.join(base, "reddit-streaming", "db_creds.json")
pp = pprint.PrettyPrinter(indent = 1)

with open("creds.json", "r") as f:
    creds = json.load(f)
    print("creds: ")
    pp.pprint(creds)
    f.close()

def get_bearer():
    """
    gets bearer token from reddit.

    returns: header for request
    """
    # with open(creds_path, "r") as f:
    #     creds = json.load(f)

    auth = requests.auth.HTTPBasicAuth(creds["client-id"], creds["secret-id"])
    data = {
            'grant_type': 'password',
            'username': creds["user"],
            'password': creds["password"]
            }
    headers = {'User-Agent': 'reddit-streaming/0.0.1'}

    response = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

    token = response.json()["access_token"]
    headers = {**headers, **{'Authorization': f"bearer {token}"}}
    return(headers)

def get_subreddit(subreddit, limit, after, headers):
    """
    gets data for a given subreddit.

    params: subreddit (str)
            limit (int)
            header (dict)

    returns: response (json)
    """
    request_url = "https://oauth.reddit.com/r/{}/hot".format(subreddit)
    options = {"limit":str(limit), "after":str(after)}
    response = requests.get(request_url, 
                            headers = headers,
                            params = options)

    try:
        response_json = response.json()
        return(response_json)
    
    except Exception as e:
        pp.pprint(e)

def serializer(message):
            return json.dumps(message).encode('utf-8')

def push_kafka(topic, subreddit):

    try:
        # print("pulling api data...")
        headers = get_bearer()
        response = get_subreddit(subreddit, 1, "", headers)
        after_token = response["data"]["after"]
        # pp.pprint(response["data"]["children"])

        broker = ["host.docker.internal:9092"]
        local_broker = ["localhost:9092"]
        # public_brokers = ["xanaxprincess.asuscomm.com:9091", "xanaxprincess.asuscomm.com:9092", "xanaxprincess.asuscomm.com:9093"]

        producer = KafkaProducer(
            bootstrap_servers=broker,
            value_serializer=serializer
        )

        producer.send(topic, response.json())

    except Exception as e:
        pp.pprint(e)
        print("reauthenticating...")
        headers = get_bearer()
        response = get_subreddit(subreddit, 1, "", headers)
        after_token = response["data"]["after"]
        # pp.pprint(response)

    try:
        # final = serializer(response)
        producer.send(topic, response)
        # print("wrote api to kafka.")
        # pp.pprint(response)

    except Exception as f:
        pp.pprint(f)

        # fname = "/home/ubuntu/reddit-streaming/redditStreaming/src/main/resources/reddit-agw-100.json"
        # with open(fname, "w") as g:
        #     json.dump(g)
        #     print("wrote json.")

def parse_data_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming == True, "DataFrame doesn't receive streaming data"

def read_spark(subreddit):

    # kafka consumer code here...

    # spark kafka
    spark = SparkSession.builder.appName(subreddit)\
            .master("spark://spark-master:7077")\
            .config("spark.eventLog.enabled", "true")\
            .getOrCreate()
            # .config("spark.eventLog.dir", "file:///opt/workspace/events")\

    try:
        KAFKA_TOPIC = "reddit"

        # Set log-level to WARN to avoid very verbose output
        spark.sparkContext.setLogLevel('WARN')

        # schema for parsing value string passed from Kafka
        testSchema = StructType([ \
                StructField("test_key", StringType()), \
                StructField("test_value", FloatType())])

        # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
        df_kafka = spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "host.docker.internal:9092") \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING)")

        # parse streaming data and apply a schema
        df_kafka = parse_data_from_kafka_message(df_kafka, testSchema)

        # query the spark streaming data-frame that has columns applied to it as defined in the schema
        # query = df_kafka.groupBy("test_key").sum("test_value")

        # df_kafka.write.format("jdbc")

        # write the output out to the console for debugging / testing
        df_kafka.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", False) \
        .start() \
        .awaitTermination()

    except Exception as e:
        pp.pprint(e)
    

def main(subreddit):

    try:
        push_kafka("reddit", subreddit)
        # read_spark(subreddit)

    except Exception as e:
        pp.pprint(e)

    while True:

        try:
            push_kafka("reddit", subreddit)
            # read_spark(subreddit)

        except Exception as e:
            pp.pprint(e)
        
        except:
            print("reauthenticating...")
            push_kafka("reddit", subreddit)
            # read_spark(subreddit)

if __name__ == "__main__":
    print("running main function reddit to kafka...")
    main("AsiansGoneWild")