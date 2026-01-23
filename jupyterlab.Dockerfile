FROM cluster-base

# -- Layer: JupyterLab with uv and virtual environment

ARG spark_version=3.5.3
ARG jupyterlab_version=4.3.4

COPY ./redditStreaming/requirements.txt ${SHARED_WORKSPACE}/redditStreaming/
COPY ./redditStreaming/ ${SHARED_WORKSPACE}/redditStreaming/

# Install PostgreSQL development libraries for psycopg2
RUN apt-get update -y && \
    apt-get install -y postgresql-server-dev-all && \
    rm -rf /var/lib/apt/lists/*

# Install uv - fast Python package installer
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:${PATH}"

# Create virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV VIRTUAL_ENV="/opt/venv"

# Set UV timeout to avoid network timeout issues
ENV UV_HTTP_TIMEOUT=300

# Install Python packages using uv
RUN uv pip install --no-cache pyspark==${spark_version} jupyterlab==${jupyterlab_version}

# Install requirements using uv
RUN uv pip install --no-cache -r /opt/workspace/redditStreaming/requirements.txt
    
# aws
RUN mkdir -p /root/.aws

# -- Runtime

EXPOSE 8888
WORKDIR ${SHARED_WORKSPACE}
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root", "--ServerApp.token=easy", "--ServerApp.password=easy", "--notebook-dir=/opt/workspace"]

