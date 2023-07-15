FROM stevenhurwitt/cluster-base:latest

ARG shared_workspace=/opt/workspace

# copy scala code
COPY redditStreaming/src/main/scala {SHARED_WORKSPACE}/src/main/scala
COPY redditStreaming/pom.xml {SHARED_WORKSPACE}/pom.xml
COPY redditStreaming/requirements.txt {SHARED_WORKSPACE}/requirements.txt
COPY redditStreaming/src/main/python/reddit/setup.py {SHARED_WORKSPACE}/setup.py


# build wheel
WORKDIR /opt/workspace
RUN python3 {SHARED_WORKSPACE}/setup.py bdist_wheel

# build uber jar
RUN cd {SHARED_WORKSPACE} && \
    mvn clean package

# ENTRYPOINT [ "python3 redditStreaming/src/main/python/reddit/reddit_streaming.py" ]