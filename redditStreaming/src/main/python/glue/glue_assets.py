import boto3
import json
import sys
import os

def glue_assets(filepath, command):
    """
    glue_assets

    move scripts to s3 glue bucket/folder.
    input:
    output:
    """

    if command not in ["upload", "download"]:
        raise ValueError("command must be either 'upload' or 'download'")
        sys.exit()

    s3 = boto3.client("s3", region_name = "us-east-2")
    my_bucket = "aws-glue-assets-965504608278-us-east-2"
    my_folder = "scripts"
    filepath = "scripts/technology-curation.py"
    local_path = "/opt/workspace/redditStreaming/src/main/python/glue/scripts/technology-curation.py"
    s3_url = "s3://{}/{}/{}".format(my_bucket, my_folder, filepath)
    print("source: {}".format(local_path))
    print("target: {}".format(s3_url))

    if command == "upload":
        # s3://aws-glue-assets-965504608278-us-east-2/scripts/technology-curation.py
        print("s3://{}/{}/{}".format(my_bucket, my_folder, filepath))

        s3.put_object(Bucket = my_bucket, Key = "{}/{}".format(my_folder, filepath), Body = open(filepath, "rb"))
        print("uploaded object to s3.")

    if command == "download":
        s3.get_object(Bucket = my_bucket, Key = "{}/{}".format(my_folder, filepath))
        print("downloaded object from s3.")

if __name__ == "__main__":

    filepath = os.environ["filepath"]
    command = "upload"
    # command = "download"

    glue_assets(filepath, command)
    print("finished glue assets run.")
