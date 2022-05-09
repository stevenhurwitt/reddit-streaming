# reddit-streaming
An attempt to stream data from the reddit api using kafka, process with spark, and store in s3 data lake.

## Build dockerfiles

Go to docker directory and run build script.

`./build.sh`

`docker-compose up -d --no-recreate`

## Start streaming data

`cd redditStreaming/src/main/python/reddit`

`python3 -m reddit_streaming.py`

## Start kafka producer

`python3 -m reddit_producer.py`

### Remove untagged docker images

`docker rmi $(docker images | grep "^<none>" | awk "{print $3}")`

### Kafka/zookeeper broker id mismatch

https://github.com/wurstmeister/kafka-docker/issues/409