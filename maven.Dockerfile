FROM stevenhurwitt/cluster-base:latest

# copy scala code
COPY redditStreaming/src/main/scala /opt/workspace/src/main/scala
COPY redditStreaming/pom.xml /opt/workspace/pom.xml
COPY redditStreaming/requirements.txt /opt/workspace/requirements.txt

# build wheel, copy
WORKDIR /opt/workspace
RUN python3 redditStreaming/src/main/python/reddit/setup.py bdist_wheel && \
    cp redditStreaming/src/main/python/reddit/dist/reddit-1.0.0-py3-none-any.whl /opt/workspace/reddit-0.1.0-py3-none-any.whl

# build uber jar, copy
RUN cd redditStreaming && \
    mvn clean package && \
    cp target/reddit-1.0-SNAPSHOT.jar /opt/workspace/reddit.jar && \
    cp target/uber-reddit-1.0-SNAPSHOT.jar /opt/workspace/uber-reddit.jar

ENTRYPOINT [ "python3 redditStreaming/src/main/python/reddit/reddit_streaming.py" ]