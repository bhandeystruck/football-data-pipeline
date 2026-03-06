FROM apache/airflow:2.9.3

# Install Python deps as the 'airflow' user (required by Airflow image)
USER airflow

RUN pip install --no-cache-dir \
    boto3 \
    python-dotenv \
    snowflake-connector-python \
    "dbt-core==1.11.7" \
    "dbt-snowflake==1.11.3"