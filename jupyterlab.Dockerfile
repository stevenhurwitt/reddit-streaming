FROM cluster-base

# -- Layer: JupyterLab

ARG spark_version=3.4.4
ARG jupyterlab_version=3.2.0

# Create required directories
RUN mkdir -p /opt/workspace/redditStreaming

# Install system dependencies
RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y \
        python3-pip \
        python3-dev \
        build-essential \
        libssl-dev \
        libffi-dev \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python packages
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir \
        pypandoc==1.5 \
        "pyspark==${spark_version}" \
        "jupyterlab==${jupyterlab_version}" \
        wget \
        numpy \
        pandas \
        psycopg2-binary \
        matplotlib

# Configure Python symlink
RUN rm -f /usr/bin/python && \
    ln -s /usr/local/bin/python3 /usr/bin/python

# Copy project files
COPY ./redditStreaming /opt/workspace/redditStreaming

# -- Runtime
EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --allow-root --NotebookApp.token=