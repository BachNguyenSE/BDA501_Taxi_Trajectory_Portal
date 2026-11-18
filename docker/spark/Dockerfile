# Python 3.12 + JDK 17 + PySpark 4.0.1 (cố định theo đề).
FROM python:3.12-slim-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

ENV PYTHONHASHSEED=42 \
    PYTHONPATH=/workspace/src \
    PYTHONDONTWRITEBYTECODE=1 \
    PYSPARK_PYTHON=python3 \
    SPARK_LOCAL_DIRS=/tmp/spark \
    TZ=UTC

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /workspace
