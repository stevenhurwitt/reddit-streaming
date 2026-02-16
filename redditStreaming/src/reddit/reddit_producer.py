import datetime as dt
import json
import os
import pprint
import sys
import time

import kafka
import requests
# import boto3
import yaml
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

pp = pprint.PrettyPrinter(indent=1)

try:
    import reddit
    print("imported reddit module.")

except:
    print("failed to import reddit module.")
    pass

pp = pprint.PrettyPrinter(indent = 1)

def get_bearer():
    """
    gets bearer token from reddit.

    returns: header for request
    """
    base = os.getcwd()

    creds_path_container = os.path.join("/opt", "workspace", "redditStreaming", "creds.json")

    # creds_dir = "/".join(base.split("/")[:-3])
    creds_path = os.path.join(base, "creds.json")

    try:
        with open(creds_path, "r") as f:
            creds = json.load(f)
            f.close()

    except FileNotFoundError:
        print("failed to find creds.json")
        with open(creds_path_container, "r") as f:
            creds = json.load(f)
            f.close()

    except:
        print("credentials file not found.")
        sys.exit()

    auth = requests.auth.HTTPBasicAuth(creds["client_id"], creds["secret_id"])
    data = {
            'grant_type': 'password',
            'username': creds["user"],
            'password': creds["password"]
            }
    headers = {'User-Agent': 'reddit-streaming/0.1.0'}

    response = requests.post('https://www.reddit.com/api/v1/access_token', auth=auth, data=data, headers=headers)

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

        if response.status_code != 200:
            print(f"Reddit API error: status {response.status_code}, response: {response.text[:200]}")
            return None

        response_json = response.json()
        return(response_json)
    
    except Exception as e:
        print(f"Error fetching subreddit {subreddit}: {e}")
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
    if response is None:
        return None, None
    
    if "data" not in response or "children" not in response["data"] or len(response["data"]["children"]) == 0:
        print(f"Empty or invalid response: {response}")
        return None, None
    
    data = response["data"]["children"][0]["data"] #subset for just the post data
    after_token = response["data"]["after"] #save "after" token to get posts after this one
    i = 0

    ## this looks really hacky, think of a better way to do this...
    try:
        #exclude nested data for schema simplicity
        data.pop("preview")
        data.pop("link_flair_richtext")
        data.pop("media_embed")
        data.pop("user_reports")
        data.pop("secure_media_embed")
        data.pop("author_flair_richtext")
        data.pop("gildings")
        data.pop("all_awardings")
        data.pop("awarders")
        data.pop("treatment_tags")
        data.pop("mod_reports")

    except:
        data.pop("link_flair_richtext")
        data.pop("media_embed")
        data.pop("user_reports")
        data.pop("secure_media_embed")
        data.pop("author_flair_richtext")
        data.pop("gildings")
        data.pop("all_awardings")
        data.pop("awarders")
        data.pop("treatment_tags")
        data.pop("mod_reports")

    return(data, after_token)

def get_broker():
    """
    create broker & producer with retry logic.
    """
    host = "kafka"
    broker = ["{}:9092".format(host)]
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka broker (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                        bootstrap_servers=broker,
                        value_serializer=my_serializer,
                        api_version_auto_timeout_ms=30000,
                        request_timeout_ms=30000,
                        retries=3,
                        acks='all'
                    )
            print("Successfully connected to Kafka broker and initialized producer.")
            return(broker, producer)
        
        except (kafka.errors.NoBrokersAvailable, Exception) as e:
            if attempt < max_retries - 1:
                print(f"Kafka broker not available yet: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 30)  # Exponential backoff, max 30s
            else:
                print("Failed to connect to Kafka broker after maximum retries.")
                print(f"Attempted to connect to: {broker}")
                sys.exit()
    
    print("Failed to initialize Kafka producer.")
    sys.exit()


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
    broker = ["{}:9092".format(host)]
    max_retries = 30
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to Kafka broker (attempt {attempt + 1}/{max_retries})...")
            producer = KafkaProducer(
                        bootstrap_servers=broker,
                        value_serializer=my_serializer,
                        api_version_auto_timeout_ms=30000,
                        request_timeout_ms=30000,
                        retries=3,
                        acks='all'
                    )
            print("Successfully connected to Kafka broker and initialized producer.")
            break
        
        except (kafka.errors.NoBrokersAvailable, Exception) as e:
            if attempt < max_retries - 1:
                print(f"Kafka broker not available yet: {e}. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay = min(retry_delay * 1.5, 30)  # Exponential backoff, max 30s
            else:
                print("Failed to connect to Kafka broker after maximum retries.")
                print(f"Attempted to connect to: {broker}")
                sys.exit()

    params = {}
    params["topic"] = ["reddit_{}".format(s) for s in subreddit]
    topic = params["topic"][index]
    # print("created topics.")

    token_list = []

    for i, s in enumerate(subreddit):
        # print("subreddit: {}".format(subreddit))
        my_response = get_subreddit(s, 1, post_type, "", header)
        my_data, after_token = subset_response(my_response)
        token_list.append(after_token)

        if after_token is not None and my_data is not None:
            try:
                future = producer.send(params["topic"][i], my_data)
                # Wait for the send to complete (with timeout)
                record_metadata = future.get(timeout=10)
                
                if debug:
                    print("subreddit: {}, post date: {}, post title: {}, token: {}. [SENT to partition {}]".format(
                        s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token, record_metadata.partition))
            except Exception as e:
                print(f"ERROR sending message for {s}: {e}")

    # Flush to ensure messages are sent
    producer.flush()
    
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
                if after_token is not None and my_data is not None:
                    try:
                        future = producer.send(params["topic"][i], my_data)
                        record_metadata = future.get(timeout=10)
                        if debug:
                            print("subreddit: {}, post date: {}, post title: {}, token: {}. [SENT]".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))
                    except Exception as e:
                        print(f"ERROR sending in loop for {s}: {e}")
                elif my_data is None:
                    # API returned empty/invalid response, keep current token
                    after_token = params["token"][i]
                
                token_list.append(after_token) 
                
                time.sleep(5)

            except json.decoder.JSONDecodeError as e:
                # when the bearer token expires (after 24 hrs), we do not receive a response
                print(f"JSONDecodeError for {s}: {e}, reauthenticating...")
                header = get_bearer()
                after_token = params["token"][i]

                next_response = get_subreddit(s, 1, post_type, after_token, header)
                my_data, after_token = subset_response(next_response)

                if after_token is not None and my_data is not None:
                    try:
                        future = producer.send(params["topic"][i], my_data)
                        record_metadata = future.get(timeout=10)
                        if debug:
                            print("subreddit: {}, post datetime: {}, post title: {}, token: {}. [SENT]".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))
                    except Exception as e:
                        print(f"ERROR in reauth send for {s}: {e}")
                else:
                    # Still empty after reauth, keep current token
                    after_token = params["token"][i]
                
                token_list.append(after_token)
                time.sleep(5)

            except IndexError:
                # this means empty response is returned, take a nap
                token_list.append(params["token"][i])
                time.sleep(3)

            except TypeError as e:
                # NoneType errors - API returned None
                print(f"TypeError for {s}: {e}, keeping current token")
                token_list.append(params["token"][i])
                time.sleep(30)

            except Exception as e:
                # catch all for api exceptions (SSL errors, ConnectionError, etc)
                print(f"Exception for {s}: {e}")
                token_list.append(params["token"][i])
                time.sleep(60)

        # Flush to ensure all messages in this cycle are sent
        producer.flush()
        
        params["token"] = token_list
        if None in token_list:
            time.sleep(5)

        else:
            time.sleep(110)
    

def main():
    """
    authenticate and poll subreddit api
    """
    print("Starting main()...")
    try:
        # base = os.getcwd()
        # config_path = "/".join(base.split("/")[:-1])
        # config_file = os.path.join(base, "config.yaml")
        
        print("Loading config...")
        with open("/opt/workspace/redditStreaming/src/reddit/config.yaml", "r") as f:
            config = yaml.safe_load(f)
            subreddit = config["subreddit"]
            post_type = config["post_type"]
            kafka_host = config["kafka_host"]
            debug = config["debug"]
            # debug = True
            f.close()
        print(f"Config loaded: {subreddit}, {post_type}, {kafka_host}, debug={debug}")
    
    except Exception as e:
        print(f"failed to find config.yaml: {e}")
        sys.exit()

    # s3, athena, secrets = aws()

    # print("s3: {}".format(s3))
    # print("athena: {}".format(athena))
    # print("secrets: {}".format(secrets))

    print("Getting bearer token...")
    my_header = get_bearer()
    print("authenticated w/ bearer token good for 24 hrs.")
    print("Sleeping for 60 seconds before hitting Reddit API...")
    time.sleep(60)
    print("Starting poll_subreddit...")
    poll_subreddit(subreddit, post_type, my_header, kafka_host, 0, True)

if __name__ == "__main__":
    # time.sleep(600)
    try:
        print("reading from api to kafka...")
        main()

    except Exception as e:
        print(e)

        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)
            subreddit = config["subreddit"]
            post_type = config["post_type"]
            kafka_host = config["kafka_host"]
            debug = config["debug"]
            # debug = True
            f.close()

        print("read config.yaml.")
        my_header = get_bearer()
        print("authenticated w/ bearer token good for 24 hrs.")
        print("Sleeping for 30 seconds before hitting Reddit API...")
        time.sleep(30)
        poll_subreddit(subreddit, post_type, my_header, kafka_host, 0, True)