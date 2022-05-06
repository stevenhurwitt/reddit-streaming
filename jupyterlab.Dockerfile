FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.2.0
ARG jupyterlab_version=3.4.0

COPY ./redditStreaming/creds.json ${SHARED_WORKSPACE}
COPY ./redditStreaming/src/main/python/requirements.txt ${SHARED_WORKSPACE}

RUN apt-get update -y && \
    apt-get install -y python3-pip && \
    python3 -m pip install --upgrade pip && \
    python3 -m pip install pyspark==${spark_version} jupyterlab==${jupyterlab_version} && \
    python3 -m pip install -r /opt/workspace/requirements.txt && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s /usr/local/bin/python3 /usr/bin/python

# moved pip3 install jupyter below rm -rf /var/lib/apt/lists/* b/c of error

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=

