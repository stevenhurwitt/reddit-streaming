FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.2.0
ARG jupyterlab_version=3.2.5

COPY ./redditStreaming/requirements.txt ${SHARED_WORKSPACE}/redditStreaming/
COPY ./redditStreaming/ ${SHARED_WORKSPACE}/redditStreaming/

# base python
RUN apt-get install debian-archive-keyring
RUN wget -O - ports.debian.org/archive_2021.key | apt-key add -
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 
# RUN mkdir /apt
# RUN mkdir /apt/etc
# RUN chmod +rwx /apt/etc
# RUN touch /apt/etc/sources.list
# RUN cat "deb http://deb.debian.org/debian/ buster/updates main contrib non-free" >> /apt/etc/sources.list
# RUN cat "deb-src http://deb.debian.org/debian/ buster/updates main contrib non-free" >> /apt/etc/sources.list

RUN apt-get update --allow-insecure-repositories -y && \
    apt-get install -y python3-dev python3-distutils python3-setuptools && \
    curl https://bootstrap.pypa.io./get-pip.py | python3 && \
    python3 -m pip install --upgrade pip
    
RUN python3 -m pip install --no-cache-dir pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# custom .whl's
# RUN python3 -m pip install /opt/workspace/redditStreaming/target/reddit-0.1.0-py3-none-any.whl --force-reinstall

# requirements
RUN python3 -m pip install --no-cache-dir -r /opt/workspace/redditStreaming/requirements.txt --ignore-installed
    
RUN rm -rf /var/lib/apt/lists/* && \
    mkdir root/.aws
    # ln -s /usr/local/bin/python3 /usr/bin/python

# deal w/ outdated pyspark guava jar for hadoop-aws (check maven repo for hadoop-common version)
RUN ls /usr/local/lib/python3.7/dist-packages/pyspark/jars/

RUN cd /usr/local/lib/python3.7/dist-packages/pyspark/jars/ && \
    rm guava-14.0.1.jar && \
    wget https://repo1.maven.org/maven2/com/google/guava/guava/27.0-jre/guava-27.0-jre.jar

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=
