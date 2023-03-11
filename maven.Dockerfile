FROM stevenhurwitt/cluster-base:latest

# copy scala code
COPY redditStreaming/src/main/scala /opt/workspace/src/main/scala
COPY redditStreaming/pom.xml /opt/workspace/pom.xml
COPY redditStreaming/requirements.txt /opt/workspace/requirements.txt
COPY redditStreaming/src/main/python/reddit/setup.py /opt/workspace/setup.py


# build wheel, copy
WORKDIR /opt/workspace
RUN python3 opt/workspace/setup.py bdist_wheel

# build uber jar, copy
RUN cd /opt/workspace/ && \
    mvn clean package

ENTRYPOINT [ "python3 redditStreaming/src/main/python/reddit/reddit_streaming.py" ]