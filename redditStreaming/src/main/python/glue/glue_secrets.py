import pprint
import boto3
import json
import os
pp = pprint.PrettyPrinter(indent = 1)

def glue_secrets():
    """
    glue_secrets
    get secrets for glue from aws

    input - None
    output - creds.json
    """
    with open("glue.json", "r") as f:
        glue = json.load(f)
        f.close()
    
    secret = boto3.client("secretmanager", region_name = "us-east-2")
    aws_client = secret.get_secret_value(SecretId = glue["AWS_ACCESS_KEY_ID"])
    aws_secret = secret.get_secret_value(SecretId = glue["AWS_SECRET_ACCESS_KEY"])
    subreddit = os.environ["subreddit"]
    print("got secret values.")

    creds = {
        "AWS_ACCESS_KEY_ID": aws_client["SecretString"],
        "AWS_SECRET_ACCESS_KEY": aws_secret["SecretString"],
        "subreddit": subreddit
    }

    with open("my_creds.json", "w") as g:
        json.dump(creds, g)
        g.close()

    print("wrote my_creds.json.")
    return(creds)

if __name__ == "__main__":

    print("running glue_secrets...")
    my_creds = glue_secrets()
    print("finished glue_secrets.")
    pp.pprint(my_creds)