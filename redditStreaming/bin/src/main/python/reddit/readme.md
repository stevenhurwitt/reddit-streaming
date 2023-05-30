# reddit producer

kafka producer reading from reddit api and publishing (json) messages.
streams eight different subreddits at once (one per core - not multithreaded in python tho...).
could stream more...

1. get_bearer()

gets the bearer token for reddit api.

2. get_subreddit()

gets data from specific reddit subreddit page.

3. my_serializer()

serializes message in utf-8.

4. subset_response()

subsets response data into subset of columns (can be modified in code).

5. poll_subreddit()

continuously polls a subreddit for latest posts. reads data from the reddit api, writes to a kafka message (json body).

6. main()

main function to run kafka producer for reddit api.

# reddit streaming

pyspark streaming docker-compose cluster to run data ingestion, curation, transformation, loading, etc.

1. read_files()

read the config and credentials files (both json).

2. init_spark()

start the spark cluster.

3. read_kafka_stream()

reads messages from kafka producer (spark streaming consumer)

4. write_stream()

write streaming data to s3 bucket as delta table.

5. main()

main function to read spark streaming data and write to s3 delta table.

# to-do

1. airflow to restart producer gracefully every X min.

2. docker-compose to aws cloud.

3. web server? python flask api?

4. databases...

5. machine learning

6. data analysis

7. conclusions