import pprint
import boto3
import json
import os

def main():
    secrets = boto3.client("secretsmanager", region_name = "us-east-2")
    val = secrets.get_secret_value(SecretId = "reddit-streaming-secrets")
    pp = pprint.PrettyPrinter(indent = 1)
    base = os.getcwd()
    print(val)
    print(base)

    with open("glue.json", "r") as f:
        glue = json.load(f)
        f.close()
        print(glue)
    print("read glue.json")

    pp.pprint(glue)
    print("glue type: {}".format(type(glue)))
    return(glue)

if __name__ == "__main__":
    pp = pprint.PrettyPrinter(indent = 1)
    glue = main()
    pp.pprint(glue)

