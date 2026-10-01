FROM apache/spark:4.0.4-scala2.13-java17-python3-r-ubuntu

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        libbz2-dev \
        libffi-dev \
        liblzma-dev \
        libreadline-dev \
        libsqlite3-dev \
        libssl-dev \
        libuuid1 \
        libgomp1 \
        uuid-dev \
        wget \
        zlib1g-dev \
    && wget -q https://www.python.org/ftp/python/3.12.15/Python-3.12.15.tgz -O /tmp/Python-3.12.15.tgz \
    && echo "de1a241a519e0a3374fea98988d0b52c886743f9d953f23be8269cc7b59c5fab  /tmp/Python-3.12.15.tgz" | sha256sum -c - \
    && tar -xzf /tmp/Python-3.12.15.tgz -C /tmp \
    && cd /tmp/Python-3.12.15 \
    && ./configure --prefix=/usr/local --with-ensurepip=install \
    && make -j4 \
    && make altinstall \
    && cd / \
    && rm -rf /tmp/Python-3.12.15 /tmp/Python-3.12.15.tgz \
    && apt-get purge -y build-essential libbz2-dev libffi-dev liblzma-dev libreadline-dev libsqlite3-dev libssl-dev uuid-dev zlib1g-dev \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY spark/requirements-benchmark.txt /tmp/requirements-benchmark.txt
COPY spark/requirements-inference.txt /tmp/requirements-inference.txt
RUN python3 -m pip install \
    --no-cache-dir \
    --no-deps \
    --target /opt/delta-python \
    -r /tmp/requirements-benchmark.txt

RUN python3.12 -m pip install \
    --no-cache-dir \
    --target /opt/inference-python \
    --index-url https://pypi.org/simple \
    -r /tmp/requirements-inference.txt

ENV PYTHONPATH=/opt/inference-python:/opt/delta-python:/opt/project:/opt/project/spark:/opt/project/spark/jobs \
    PYSPARK_PYTHON=/usr/local/bin/python3.12 \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python3.12

USER spark
