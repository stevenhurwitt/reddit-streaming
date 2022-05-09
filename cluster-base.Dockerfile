ARG debian_buster_image_tag=8-jdk-buster
FROM openjdk:${debian_buster_image_tag}

# -- Layer: OS + Python 3.7

ARG shared_workspace=/opt/workspace

RUN rm /bin/sh && ln -s /bin/bash /bin/sh

RUN mkdir -p ${shared_workspace} && \
    apt-get update -y && \
	apt install -y curl gcc zip unzip telnet &&\ 
	apt install -y build-essential zlib1g-dev libncurses5-dev && \
	apt install -y libsqlite3-dev && \
	apt install -y libgdbm-dev libnss3-dev libssl-dev libreadline-dev libffi-dev wget libjpeg-dev && \
    apt install -y python3 && \
	# curl -O https://www.python.org/ftp/python/3.7.3/Python-3.7.3.tar.xz  && \
    # tar -xf Python-3.7.3.tar.xz && cd Python-3.7.3 && ./configure && make -j 8 &&\
    # make install && \
    apt-get update && apt-get install -y procps && apt-get install -y nano && apt-get install -y net-tools && \
    rm -rf /var/lib/apt/lists/*

# sdkman for scala & maven
RUN curl -s https://get.sdkman.io | bash
RUN chmod a+x "$HOME/.sdkman/bin/sdkman-init.sh" && \
    source "$HOME/.sdkman/bin/sdkman-init.sh" && \
    sdk install maven && \
    sdk install scala 2.12.15 && \
    sdk use scala 2.12.15

# almond.sh for scala jupyter kernel
RUN curl -Lo coursier https://git.io/coursier-cli && \
    chmod +x coursier && \
    ./coursier --help

RUN ./coursier launch --fork almond:0.11.1 -v -v --scala 2.12 -- --install && \
    rm -f coursier

ENV SHARED_WORKSPACE=${shared_workspace}

# -- Runtime

VOLUME ${shared_workspace}
CMD ["bash"]
