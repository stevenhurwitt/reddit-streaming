import datetime as dt
import pandas as pd
import numpy as np
# import yaml
import json
import time
import sys
import os

def main(subreddit):

    # get current working directory
    base = os.getcwd()
    creds = "creds.json"
    # config = "config.yaml"

    with open(creds, "r") as f:
        creds_file = json.load(f)
        f.close()
        os.environ["AWS_ACCESS_KEY_ID"] = creds_file["aws-client"]
        os.environ["AWS_SECRET_ID"] = creds_file["aws-secret"]
        # os.environ["subreddit"] = "subreddit"
        print("read in aws key & secret.")

    # with open(config, "r") as g:
    #     config_file = yaml.load(g)

    # start time
    start_time = time.time()
    start_datetime = dt.datetime.now()

    print("working in {}".format(base))
    print("starting time: {}".format(start_time))
    print("starting datetime: {}".format(start_datetime))

    # set subreddit
    os.environ["subreddit"] = subreddit
    print("set subreddit to {}.".format(subreddit))
    print("os['subreddit'): subreddit")
    print("subreddit: {}".format(os.environ["subreddit"]))
    sys.exit()



if __name__ == "__main__":

    with open("subreddit.json", "r") as h:
        subreddit = json.load(h)
        print("read subreddit: {}".format(subreddit))

    main(subreddit)
    print("set subreddit to {} successfull.".format(subreddit))

    pass
