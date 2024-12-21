FROM python:3.8

ENV PROTOBUF_VERSION=3.19.4
# cd whylogs
# docker build -t whylogs -f Dockerfile .
# docker run --rm -it -p 8080:8888 -v /working/directory:/workspace whylogs

RUN mkdir /workspace
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
RUN apt-get update && apt-get install apt-utils -y -q
RUN apt-get update && \
    apt-get install git -y && \
    apt-get install awscli -y && \
    apt-get install sudo -y && \
    adduser --quiet --disabled-password --gecos "" whyuser && \
    adduser whyuser sudo && \
    echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN curl -sLJO https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    unzip protoc-*-linux-x86_64.zip -d /usr && \
    chmod -R a+rx /usr/bin/ /usr/include/google
RUN apt-get update && \
    apt-get install -y cmake openjdk-17-jre-headless graphviz && \
    pip install --root-user-action ignore --upgrade pip && \
    pip install --root-user-action ignore pytest && \
    pip install --root-user-action ignore pytest-cov && \
    pip install --root-user-action ignore jupyterlab && \
    pip install --root-user-action ignore numpy && \
    pip install --root-user-action ignore pandas && \
    pip install --root-user-action ignore sphinx
RUN curl -fsSL https://deb.nodesource.com/setup_14.x | bash - && \
    apt-get update && apt-get install nodejs npm -y
RUN npm install --global yarn

RUN apt-get update && apt-get install -y less emacs vim

RUN pip install --root-user-action ignore ruff

USER whyuser

WORKDIR /home/whyuser
RUN curl -sSL https://install.python-poetry.org | python3 - --version 1.6.1 && \
    echo 'export PATH="$PATH:$HOME/.local/bin"' >> .bashrc

WORKDIR /workspace
CMD [ "bash" ]
