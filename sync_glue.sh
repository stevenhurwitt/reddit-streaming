#!/bin/bash

# Sync the Glue scripts to S3
aws s3 sync redditStreaming/src/main/python/scripts/. s3://reddit-streaming-stevenhurwitt/scripts/