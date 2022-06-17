FROM stevenhurwitt/cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.2.0
ARG jupyterlab_version=3.2.5

COPY ./redditStreaming/creds.json ${SHARED_WORKSPACE}
COPY ./redditStreaming/src/main/python/requirements.txt ${SHARED_WORKSPACE}
COPY ./redditStreaming/fairscheduler.xml ${SHARED_WORKSPACE}

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install --no-cache-dir pyspark==${spark_version} jupyterlab==${jupyterlab_version} && \
    python3 -m pip install --no-cache-dir -r /opt/workspace/requirements.txt --ignore-installed && \
    rm -rf /var/lib/apt/lists/*
    # ln -s /usr/local/bin/python3 /usr/bin/python

# deal w/ outdated pyspark guava jar for hadoop-aws (check maven repo for hadoop-common version)
RUN cd /usr/local/lib/python3.7/dist-packages/pyspark/jars/ && \
    mv guava-14.0.1.jar guava-14.0.1.jar.bk && \
    wget https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar
# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=

