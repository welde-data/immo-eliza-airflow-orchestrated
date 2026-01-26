FROM apache/airflow:2.9.0-python3.10

USER root

# system deps...
RUN apt-get update && apt-get install -y wget gnupg unzip curl xvfb \
    && rm -rf /var/lib/apt/lists/*

# install chrome...
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" \
       > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# critical: remove any chromedriver baked into base / older layers
RUN rm -f /usr/local/bin/chromedriver || true

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
