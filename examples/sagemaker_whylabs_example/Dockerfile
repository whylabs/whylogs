FROM ubuntu:18.04

ENV MODEL_SERVER_TIMEOUT=180

# Install python and other scikit-learn runtime dependencies
# Dependency list from http://scikit-learn.org/stable/developers/advanced_installation.html#installing-build-dependencies
RUN apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install --no-install-recommends \
        build-essential \
        curl \
        git \
        jq \
        libatlas-base-dev \
        python \
        python3-pip \
        nginx \
        openjdk-8-jdk-headless \
        unzip \
        wget \
        ca-certificates \
        libssl-dev libffi-dev \
        python3-setuptools \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install pip --upgrade

# Python won’t try to write .pyc or .pyo files on the import of source modules
# Force stdin, stdout and stderr to be totally unbuffered. Good for logging
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONIOENCODING=UTF-8 LANG=C.UTF-8 LC_ALL=C.UTF-8
ENV PATH="/opt/program:${PATH}"

COPY requirements.txt /requirements.txt
RUN pip3 install -r /requirements.txt && \
    rm /requirements.txt

# Create directory for models
RUN mkdir -p /opt/ml/models
RUN chmod +rwx /opt/ml/models

# forward request and error logs to docker log collector
RUN ln -sf /dev/stdout /var/log/nginx/access.log
RUN ln -sf /dev/stderr /var/log/nginx/error.log

# Set up the program in the image
COPY code /opt/program
WORKDIR /opt/program

EXPOSE 8080
