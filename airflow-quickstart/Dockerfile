FROM apache/airflow:2.9.3

# Install git in case dbt packages are pulled via git (packages.yml)
USER root
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*
USER airflow

# Install dbt adapter(s)
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt