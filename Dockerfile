FROM apache/airflow:2.8.4-python3.10

USER root
# Install system-level build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements into the container
COPY requirements.txt /requirements.txt

# Install using the --user flag to avoid permission errors
# and ensure compatibility with the Airflow home directory
RUN pip install --no-cache-dir --user -r /requirements.txt