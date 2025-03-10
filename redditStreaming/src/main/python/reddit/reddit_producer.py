import os
import sys
import ast
import time
import json
import yaml
import pprint
import requests
import logging

import kafka
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from kafka import KafkaProducer
import datetime as dt
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

pp = pprint.PrettyPrinter(indent=1)

from pyspark.sql import SparkSession

# Replace the existing SparkContext initialization with:
spark = SparkSession.builder \
    .appName("RedditProducer") \
    .getOrCreate()
sc = spark.sparkContext

logger = logging.getLogger('reddit_producer')

subreddit = "aws"

# secretmanager_client = boto3.client("secretsmanager")

delta_version = "2.2.0"
spark_version = "3.4.0"
hadoop_version = "3.3.4"
postgres_version = "42.5.0"
# aws_client = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"])["AWS_ACCESS_KEY_ID"]
# aws_secret = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"])["AWS_SECRET_ACCESS_KEY"]
extra_jar_list = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},org.apache.hadoop:hadoop-common:{hadoop_version},org.apache.hadoop:hadoop-aws:{hadoop_version},org.apache.hadoop:hadoop-client:{hadoop_version},io.delta:delta-core_2.12:{delta_version},org.postgresql:postgresql:{postgres_version}"
bucket = "reddit-streaming-stevenhurwitt-2"


try:
    import reddit
    print("imported reddit module.")

except:
    print("failed to import reddit module.")
    pass

pp = pprint.PrettyPrinter(indent = 1)

# def aws():
#     s3_client = boto3.client("s3")
#     athena_client = boto3.client("athena")
#     secret_client = boto3.client("secrets")
#     return(s3_client, athena_client, secret_client)

def get_bearer():
    """
    gets bearer token from reddit.

    returns: header for request
    """
    base = os.getcwd()

    creds_path_container = os.path.join("/opt", "workspace", "redditStreaming", "creds.json")

    creds_dir = "/".join(base.split("/")[:-3])
    creds_path = os.path.join(creds_dir, "creds.json")

    try:
        with open(creds_path, "r") as f:
            creds = json.load(f)
            f.close()

    except FileNotFoundError:
        with open(creds_path_container, "r") as f:
            creds = json.load(f)
            f.close()

    except:
        print("credentials file not found.")
        sys.exit()

    auth = requests.auth.HTTPBasicAuth(creds["client-id"], creds["secret-id"])
    data = {
            'grant_type': 'password',
            'username': creds["user"],
            'password': creds["password"]
            }
    headers = {'User-Agent': 'reddit-streaming/0.0.1'}

    response = requests.post('https://www.reddit.com/api/v1/access_token',
                    auth=auth, data=data, headers=headers)

    try:
        token = response.json()["access_token"]
        headers = {**headers, **{'Authorization': f"bearer {token}"}}
        return(headers)

    except Exception as e:
        print(e)
        print(response.json())
        pass


def get_subreddit(subreddit, limit, post_type, before, headers):
    """
    gets data for a given subreddit.

    params: subreddit (str) - name of subreddit
            limit (int) - number of results to return
            post_type (str) - type of posts (hot, new, controversial, top, etc)
            header (dict) - request header w/ bearer token

    returns: response (json) - body of api response
    """

    request_url = "https://oauth.reddit.com/r/{}/{}".format(subreddit, post_type)
    options = {"limit":str(limit), "before":str(before)}
    try:
        response = requests.get(request_url, 
                            headers = headers,
                            params = options)

        # Check if request was successful
        response.raise_for_status()

        response_json = response.json()
        # Validate response structure
        if "data" not in response_json:
            print(f"Unexpected API response for {subreddit}:")
            pp.pprint(response_json)
            return None
        
        return(response_json)
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from Reddit API: {str(e)}")
        return None

def my_serializer(message):
    return json.dumps(message).encode('utf-8')

def subset_response(response):
    """
    remove nested data structures from response data

    params:
        response (json)

    return:
        data (dict)
        after_token (str)
    """
    if not response or "data" not in response:
        return None, None
    
    try:
        data = response["data"]["children"][0]["data"] #subset for just the post data
        after_token = response["data"]["after"] #save "after" token to get posts after this one
        i = 0

    except (KeyError, IndexError) as e:
        print(f"Error parsing Reddit response: {str(e)}")
        print("Response structure:")
        pp.pprint(response)
        return None, None

    ## this looks really hacky, think of a better way to do this...
    try:
        # Remove nested fields
        fields_to_remove = [
            "preview", "link_flair_richtext", "media_embed", 
            "user_reports", "secure_media_embed", "author_flair_richtext",
            "gildings", "all_awardings", "awarders", 
            "treatment_tags", "mod_reports"
        ]
        
        for field in fields_to_remove:
            data.pop(field, None)  # Use pop with None default to avoid KeyError

        return(data, after_token)
    
    except Exception as e:
        print(f"Error cleaning response data: {str(e)}")
        return None, None

def poll_subreddit(subreddit, post_type, header, host, index, debug):
    """
    infinite loop to poll api & push new responses to kafka

    params:
        subreddit (str) - name of subreddit
        post_type (str) - type of posts (new, hot, controversial, etc)
        header (dict) - request header w/ bearer token
        host (str) - kafka host name
        port (int) - kafka port num
        debug (bool) - debug mode (True/False)

    """
    retries = 5
    while retries > 0:
        try:
            broker = [f"{host}:9092"]
            if not check_kafka_connection(host):
                # Fallback to IP if hostname fails
                broker = ["172.17.0.1:9092"]  # Adjust IP as needed
                
            producer = KafkaProducer(
                        bootstrap_servers=broker,
                        value_serializer=my_serializer,
                        retries=5,
                        retry_backoff_ms=1000,
                        request_timeout_ms=30000,
                        api_version=(0, 10, 2)
            )
            
            if debug:
                print(f"Connected to Kafka broker at {host}:9092")
            break
        
        except kafka.errors.NoBrokersAvailable:
            retries -= 1
            if retries == 0:
                print(f"Failed to connect to Kafka after 5 attempts")
                sys.exit(1)
            
            print(f"Retrying Kafka connection... ({retries} attempts left)")
            time.sleep(5)
            print(f"Error connecting to Kafka broker at {host}:9092")
            print(f"Error details: {str(e)}")
            sys.exit(1)

    params = {}
    params["topic"] = ["reddit_{}".format(s) for s in subreddit]
    topic = params["topic"][index]

    token_list = []

    for i, s in enumerate(subreddit):
        my_response = get_subreddit(s, 1, post_type, "", header)
        my_data, after_token = subset_response(my_response)
        
        if my_data is None:
            print(f"Failed to get initial data for subreddit: {s}")
            token_list.append(None)
            continue

        token_list.append(after_token)
        # with open("sample_response.json", "w") as f:
        #     json.dump(my_data, f, indent = 1)

        if after_token is not None:
            try:
                producer.send(params["topic"][i], my_data)
                if debug:
                    print(f"subreddit: {s}, post date: {dt.datetime.fromtimestamp(my_data['created'])}, post title: {my_data['title']}, token: {after_token}.")
            except Exception as e:
                print(f"Error sending data to Kafka: {str(e)}")
    
            if debug:
                print("subreddit: {}, post date: {}, post title: {}, token: {}.".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))

    params["token"] = token_list
    if None in token_list:
        time.sleep(5)

    else:
        time.sleep(30)

    while True:
        token_list = []
        for i, s in enumerate(subreddit):
            after_token = params["token"][i]
            try:
                next_response = get_subreddit(s, 1, post_type, after_token, header)
                my_data, after_token = subset_response(next_response)

                ## weird bug where it hits the api too fast(?) and no after token is returned
                ## this passes None, which gives the current post & correct access token
                if after_token is not None:
                    producer.send(params["topic"][i], my_data)

                    if debug:
                        print("subreddit: {}, post date: {}, post title: {}, token: {}.".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))
                
                token_list.append(after_token) 
                
                time.sleep(5)

            except json.decoder.JSONDecodeError:
                # when the bearer token expires (after 24 hrs), we do not receive a response
                print("bearer token expired, reauthenticating...")
                header = get_bearer()
                after_token = params["token"][i]

                next_response = get_subreddit(s, 1, post_type, after_token, header)
                my_data, after_token = subset_response(next_response)

                if after_token is not None:
                    producer.send(params["topic"][i], my_data)

                    if debug:
                        print("subreddit: {}, post datetime: {}, post title: {}, token: {}.".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))
                
                token_list.append(after_token)
                time.sleep(5)
                pass

            except IndexError:
                # this means empty response is returned, take a nap
                # time.sleep(120)
                # print("no more data for subreddit: {}.".format(s))
                token_list.append(params["token"][i])
                time.sleep(3)
                pass

            except Exception as e:
                # catch all for api exceptions (SSL errors, ConnectionError, etc)
                print(e)
                token_list.append(params["token"][i])
                # pass
                time.sleep(60)
                pass

        params["token"] = token_list
        if None in token_list:
            time.sleep(5)

        else:
            time.sleep(110)

def check_kafka_connection(host):
    """
    Check if Kafka broker is accessible
    """
    import socket
    import subprocess

    print(f"Checking Kafka connection to {host}...")
    
    try:
        with open('/proc/1/cgroup', 'r') as f:
            is_docker = 'docker' in f.read()
            print(f"Running in Docker container: {is_docker}")
    except:
        is_docker = False
        print("Could not determine if running in Docker")

    # Try DNS resolution first
    try:
        ip_address = socket.gethostbyname(host)
        print(f"Successfully resolved {host} to {ip_address}")
    except socket.gaierror as e:
        print(f"DNS resolution failed for {host}: {str(e)}")

        # Try ping
        try:
            result = subprocess.run(['ping', '-c', '1', host], 
                                capture_output=True, 
                                text=True)
            print(f"Ping result:\n{result.stdout}")
        except subprocess.SubprocessError as e:
            print(f"Ping failed: {str(e)}")
            return False

        # Try connection
        try:
            sock = socket.create_connection((host, 9092), timeout=5)
            sock.close()
            print(f"Successfully connected to Kafka at {host}:9092")
            return True

        except (socket.timeout, socket.error) as e:
            print(f"Failed to connect to Kafka at {host}:9092")
            print(f"Error: {str(e)}")
            return False

def main():
    """
    authenticate and poll subreddit api
    """
    try:
        # base = os.getcwd()
        # config_path = "/".join(base.split("/")[:-1])
        # config_file = os.path.join(base, "config.yaml")
        
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            subreddit = config["subreddit"]
            post_type = config["post_type"]
            kafka_host = config["kafka_host"]
            debug = config["debug"]
            # debug = True
            f.close()

        # Validate configuration
        if not subreddit or not isinstance(subreddit, list):
            raise ValueError("Invalid subreddit configuration")
            
        if not post_type in ["new", "hot", "rising", "controversial", "top"]:
            raise ValueError(f"Invalid post_type: {post_type}")

        if not check_kafka_connection(kafka_host):
            print("Kafka broker is not accessible")
            sys.exit(1)

        my_header = get_bearer()
        if not my_header:
            raise ValueError("Failed to get bearer token")
            
        if debug:
            print("authenticated w/ bearer token good for 24 hrs.")
        
        poll_subreddit(subreddit, post_type, my_header, kafka_host, 0, True)
    
    except Exception as e:
        print(f"Error in main: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    # time.sleep(600)
    print("reading from api to kafka...")
    main()