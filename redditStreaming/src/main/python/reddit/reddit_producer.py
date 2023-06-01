import os
import sys
import ast
import time
import json
import yaml
import pprint

import datetime as dt
import requests
import boto3
import kafka
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

pp = pprint.PrettyPrinter(indent=1)

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
sc.setLogLevel('INFO')
logger = glueContext.get_logger()
logger = logging.getLogger('reddit_producer')
# spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

subreddit = "aws"

secretmanager_client = boto3.client("secretsmanager")

delta_version = "2.2.0"
spark_version = "3.4.0"
hadoop_version = "3.3.4"
postgres_version = "42.5.0"
aws_client = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_ACCESS_KEY_ID")["SecretString"])["AWS_ACCESS_KEY_ID"]
aws_secret = ast.literal_eval(secretmanager_client.get_secret_value(SecretId="AWS_SECRET_ACCESS_KEY")["SecretString"])["AWS_SECRET_ACCESS_KEY"]
extra_jar_list = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{spark_version},org.apache.hadoop:hadoop-common:{hadoop_version},org.apache.hadoop:hadoop-aws:{hadoop_version},org.apache.hadoop:hadoop-client:{hadoop_version},io.delta:delta-core_2.12:{delta_version},org.postgresql:postgresql:{postgres_version}"
bucket = "reddit-streaming-stevenhurwitt-2"


try:
    import reddit
    logger.info("imported reddit module.")

except:
    logger.info("failed to import reddit module.")
    pass

pp = pprint.PrettyPrinter(indent = 1)

def aws():
    s3_client = boto3.client("s3")
    athena_client = boto3.client("athena")
    secret_client = boto3.client("secrets")
    return(s3_client, athena_client, secret_client)

def get_bearer():
    """
    gets bearer token from reddit.

    returns: header for request
    """
    base = os.getcwd()

    creds_path_container = os.path.join("/opt", "workspace", "redditStreaming", "creds.json")

    creds_dir = "/".join(base.split("/")[:-3])
    creds_path = os.path.join(creds_dir, "creds.json")
    logger.info("creds path: {}".format(creds_path))

    try:
        with open(creds_path, "r") as f:
            creds = json.load(f)
            f.close()
            logger.info("opened creds.json.")

    except FileNotFoundError:
        with open(creds_path_container, "r") as f:
            creds = json.load(f)
            f.close()
            logger.warn(FileNotFoundError)

    except:
        logger.warn("credentials file not found.")
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
        logger.info("made initial authentication request.")
        return(headers)

    except Exception as e:
        logger.warn(e)
        logger.info(response.json())
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

    logger.info("requesting {}".format(request_url))
    try:
        response = requests.get(request_url, 
                            headers = headers,
                            params = options)

        response_json = response.json()
        return(response_json)
    
    except Exception as e:
        pp.pprint(e)

def my_serializer(message):
    logger.info("serialized message to utf-8.")
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

    logger.info("subset df.")
    return(data, after_token)

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
    try:
        broker = ["{}:9092".format(host)]
        # topic = "reddit_" + subreddit

        producer = KafkaProducer(
                    bootstrap_servers=broker,
                    value_serializer=my_serializer
                    # api_version = (0, 10, 2)
                )

        logger.info("created kafka producer.")
    
    except kafka.errors.NoBrokersAvailable:
        logger.warn("no kafka broker available.")
        sys.exit()

    params = {}
    params["topic"] = ["reddit_{}".format(s) for s in subreddit]
    topic = params["topic"][index]

    token_list = []

    for i, s in enumerate(subreddit):
        logger.info("polling subreddit: {}".format(s))
        my_response = get_subreddit(s, 1, post_type, "", header)
        my_data, after_token = subset_response(my_response)
        token_list.append(after_token)
        # with open("sample_response.json", "w") as f:
        #     json.dump(my_data, f, indent = 1)

        if after_token is not None:
            producer.send(params["topic"][i], my_data)                          

            if debug:
                logger.info("subreddit: {}, post date: {}, post title: {}, token: {}.".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))

    params["token"] = token_list
    if None in token_list:
        time.sleep(5)
        logger.info("sleeping 5 seconds...")

    else:
        time.sleep(30)
        logger.info("sleeping 30 seconds...")

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
                        logger.info("subreddit: {}, post date: {}, post title: {}, token: {}.".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))
                
                token_list.append(after_token) 
                
                logger.info("sleeping 5 seconds...")
                time.sleep(5)

            except json.decoder.JSONDecodeError:
                # when the bearer token expires (after 24 hrs), we do not receive a response
                logger.warn("bearer token expired, reauthenticating...")
                header = get_bearer()
                after_token = params["token"][i]

                next_response = get_subreddit(s, 1, post_type, after_token, header)
                my_data, after_token = subset_response(next_response)

                if after_token is not None:
                    producer.send(params["topic"][i], my_data)

                    if debug:
                        logger.info("subreddit: {}, post datetime: {}, post title: {}, token: {}.".format(s, dt.datetime.fromtimestamp(my_data["created"]), my_data["title"], after_token))
                
                token_list.append(after_token)
                logger.info("sleeping 5 seconds...")
                time.sleep(5)
                pass

            except IndexError as e:
                # this means empty response is returned, take a nap
                # time.sleep(120)
                # print("no more data for subreddit: {}.".format(s))
                logger.warn(e)
                token_list.append(params["token"][i])
                logger.info("sleeping 3 seconds...")
                time.sleep(3)
                pass

            except Exception as e:
                # catch all for api exceptions (SSL errors, ConnectionError, etc)
                logger.warn(e)
                token_list.append(params["token"][i])
                # pass
                logger.info("sleeping 60 seconds...")
                time.sleep(60)
                pass

        params["token"] = token_list
        if None in token_list:
            logger.info("sleeping 5 seconds...")
            time.sleep(5)

        else:
            logger.info("sleeping 110 seconds...")
            time.sleep(110)
    

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
            logger.info("read config.yaml.")
    
    except:
        logger.warn("failed to find config.yaml")
        sys.exit()

    # s3, athena, secrets = aws()

    # print("s3: {}".format(s3))
    # print("athena: {}".format(athena))
    # print("secrets: {}".format(secrets))

    my_header = get_bearer()
    if debug:
        logger.info("authenticated w/ bearer token good for 24 hrs.")
        
    poll_subreddit(subreddit, post_type, my_header, kafka_host, 0, True)

if __name__ == "__main__":
    # time.sleep(600)
    logger.info("reading from api to kafka...")
    main()