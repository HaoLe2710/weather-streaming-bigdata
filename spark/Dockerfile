FROM apache/spark:4.0.4-scala2.13-java17-python3-r-ubuntu

USER root

COPY spark/requirements-benchmark.txt /tmp/requirements-benchmark.txt
RUN python3 -m pip install \
    --no-cache-dir \
    --no-deps \
    --target /opt/delta-python \
    -r /tmp/requirements-benchmark.txt

ENV PYTHONPATH=/opt/delta-python

USER spark
