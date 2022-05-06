from kafka import KafkaProducer
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import datetime as dt
import requests
import pprint
import time
import json
import sys
import os

pp = pprint.PrettyPrinter(indent = 1)

def get_bearer():
    """
    gets bearer token from reddit.

    returns: header for request
    """
    base = os.getcwd()

    creds_path_container = os.path.join("/opt", "workspace", "redditStreaming", "creds.json")

    creds_dir = "/".join(base.split("/")[:-4])
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

        response_json = response.json()
        return(response_json)
    
    except Exception as e:
        pp.pprint(e)

def my_serializer(message):
            # lambda v: bytes(json.dumps(v, default=str).encode('utf-8'))
            #  json.dumps(message).encode('utf-8')
            return json.dumps(message).encode('utf-8')

def poll_subreddit(subreddit, post_type, header, debug):
    """
    infinite loop to poll api & push new responses to kafka

    params:
        subreddit (str) - name of subreddit
        post_type (str) - type of posts (new, hot, controversial, etc)
        header (dict) - request header w/ bearer token
        debug (bool) - debug mode (True/False)

    """
    broker = ["kafka:9092"]
    topic = "reddit_" + subreddit

    producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=my_serializer
            )

    my_response = get_subreddit(subreddit, 1, post_type, "", header)
    my_data = my_response["data"]["children"][0]["data"] #subset for just the post data
    my_data.pop("preview") #exclude image preview for schema simplicity
    after_token = my_response["data"]["after"]
    producer.send(topic, my_data)

    if debug:
         print("post datetime: {}, post title: {}".format(dt.datetime.fromtimestamp(my_data["created"]), my_data["title"]))


    while True:
        try:
            next_response = get_subreddit(subreddit, 1, post_type, after_token, header)
            my_data = next_response["data"]["children"][0]["data"] #subset for just the post data
            my_data.pop("preview") #exclude image preview for schema simplicity
            after_token = next_response["data"]["after"]
            producer.send(topic, my_data)

            if debug:
                print("post datetime: {}, post title: {}".format(dt.datetime.fromtimestamp(my_data["created"]), my_data["title"]))
            time.sleep(10)

        except json.decoder.JSONDecodeError:
            print("bearer token expired, reauthenticating...")
            header = get_bearer()
            next_response = get_subreddit(subreddit, 1, post_type, after_token, header)
            my_data = next_response["data"]["children"][0]["data"] #subset for just the post data
            my_data.pop("preview") #exclude image preview for schema simplicity
            after_token = next_response["data"]["after"]

            if debug:
                print("post datetime: {}, post title: {}".format(dt.datetime.fromtimestamp(my_data["created"]), my_data["title"]))

        except IndexError:
            time.sleep(60)
    

def main(subreddit):
    """
    authenticate and poll subreddit api

    params:
        subreddit (str) - subreddit to read posts from
    
    """

    pp = pprint.PrettyPrinter(indent = 1)
    post_type = "new"
    my_header = get_bearer()
    poll_subreddit(subreddit, post_type, my_header, True)

if __name__ == "__main__":
    print("running main function reddit to kafka...")
    main("technology")