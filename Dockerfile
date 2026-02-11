FROM apache/airflow:2.7.0-python3.10

USER airflow
RUN pip install --no-cache-dir \
    transformers \
    torch \
    sqlalchemy \
    boto3 \
    requests
