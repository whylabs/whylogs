FROM python:3.7

RUN apt update && apt install git -y

RUN mkdir /workspace
WORKDIR /workspace
COPY . .

RUN pip3 install . && \
        python setup.py install

FROM python:3.7-slim
COPY --from=0 /usr/local/lib/python3.7/site-packages /usr/local/lib/python3.7/site-packages

RUN mkdir -p /opt/whylabs
COPY scripts/profiler.py /opt/whylabs/
WORKDIR /opt/whylabs
