FROM python:3.12-slim

WORKDIR /opt/project

COPY producer/requirements.txt /tmp/producer-requirements.txt
RUN python -m pip install --no-cache-dir -r /tmp/producer-requirements.txt \
    && rm -f /tmp/producer-requirements.txt

ENV PYTHONPATH=/opt/project \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "/opt/project/producer/live_hourly_weather_producer.py"]
CMD ["--mode", "daemon"]
