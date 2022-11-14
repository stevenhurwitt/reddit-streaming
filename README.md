# reddit-streaming

An attempt to stream multiple subreddits from the reddit api using kafka & spark, and store in s3 data lake as delta tables. nightly glue jobs processes raw data into clean data partitioned by year/month/day in athena.x


## Build dockerfiles

Go to docker directory and run build script.

`./build.sh`

Run docker-compose.

`docker-compose up -d --no-recreate`

Access jupyterlab shell. Can also attach VSCode to the jupyterlab container.

`docker exec -it jupyterlab bash`


## Start streaming data

Activate the virtual environment.

`source redditStreaming/reddit-env/bin/activate`

Go to reddit directory.

`cd redditStreaming/src/reddit`

Start pyspark streaming application.

`python3 -m reddit_streaming.py`


### Common errors:

- java gateway exited before sending port number: make sure java home is set, java -version is 1.8


## Start kafka producer

Start the kafka producer.

`python3 -m reddit_producer.py`


### Common errors:

- stored cluster id xyz does not match: go to /cluster_config/kafka/logs/metadata.properties and change to correct cluster id


### Remove untagged docker images

Remove untagged docker images.

`docker rmi $(docker images | grep "^<none>" | awk "{print $3}")`

Prune docker system volumes, containers & images.

`docker system prune && docker volume prune && docker container prune && docker image prune`


### Note on versions

When changing version of spark, hadoop, jupyterlab, etc, versions must be updated in `build.sh`, respective `*.Dockerfile`, `requirements.txt` and `reddit_streaming.py`.


### pyspark write stream to s3 error

Likely caused by guava jar mismatch, follow steps here: https://kontext.tech/article/689/pyspark-read-file-in-google-cloud-storage


### Kafka/zookeeper broker id mismatch

https://github.com/wurstmeister/kafka-docker/issues/409

If there are kafka errors, run `docker-compose down`, delete `cluster_config/kafka/logs` and `cluster_config/zookeeper/data/version-2` directories, run `docker-compose up -d`.


## S3

s3 artifact directory.

`s3://aws-glue-assets-965504608278-us-east-2/scripts/`


### aws s3 sync

configure aws cli first.

`aws configure`


### upload

`aws s3 sync redditStreaming/src/main/python/scripts/ s3://reddit-streaming-stevenhurwitt/scripts/`


### download

`aws s3 sync s3://reddit-streaming-stevenhurwitt/scripts/ redditStreaming/src/main/python/scripts/.`


## github actions

https://github.com/stevenhurwitt/reddit-streaming/actions


### pipelines

- `python.yml`
- `docker.yml`
- `terraform.yml`
- `aws.yml`


### python

Build wheel file.

`cd redditStreaming/src/main/python/reddit && python3 setup.py bdist_wheel`


### docker

Docker Compose.

`docker-compose down && ./build.sh && docker-compose up -d`


## jupyterlab shell.

`docker exec -it jupyterlab bash`


## terraform

`cd terraform && tf plan -out tfplan && tf apply tfplan`