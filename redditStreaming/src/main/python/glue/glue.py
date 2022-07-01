import pprint
import json
import time
import sys
import os

pp = pprint.PrettyPrinter(indent = 1)
filename = "creds.json"
base = os.getcwd()
with open(os.path.join(base, filename), "r") as f:
    creds = json.load(f)
    f.close()
    print("read creds.json successfully.")

params = dict()
params["subreddit"] = os.environ["subreddit"]
params["aws_client"] = os.environ["AWS_ACCESS_KEY_ID"]
params["aws_secret"] = os.environ["AWS_SECRET_ACCESS_KEY"]

subreddit = os.environ["subbreddit"]
print("subreddit: {}".format(subreddit))

def glue():
    print("running glue...")
    pp.pprint(params)
    pp.pprint(creds)
    sys.exit()

if __name__ == "__main__":
    print("starting glue.")
    glue()