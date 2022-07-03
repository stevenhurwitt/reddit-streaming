import json
import time
import sys
import os

def main():
    print("main...")
    os.environ["subreddit"] = "AsiansGoneWild"
    os.environ["secret_name"] = "secret_name"
    os.environ["AWS_ACCESS_KEY_ID"] = "AWS_ACCESS_KEY_ID"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "AWS_SECRET_ACCESS_KEY"
    print("set os environment variables.")
    sys.exit()

if __name__ == "__main__":
    print("starting main.")
    main()