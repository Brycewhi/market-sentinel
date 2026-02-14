FROM apache/airflow:2.7.0-python3.10

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install all packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Create dbt profiles directory and add configuration
RUN mkdir -p /home/airflow/.dbt && \
    echo "market_sentinel:" > /home/airflow/.dbt/profiles.yml && \
    echo "  outputs:" >> /home/airflow/.dbt/profiles.yml && \
    echo "    dev:" >> /home/airflow/.dbt/profiles.yml && \
    echo "      type: postgres" >> /home/airflow/.dbt/profiles.yml && \
    echo "      host: postgres" >> /home/airflow/.dbt/profiles.yml && \
    echo "      user: market_sentinel" >> /home/airflow/.dbt/profiles.yml && \
    echo "      password: market_sentinel_password" >> /home/airflow/.dbt/profiles.yml && \
    echo "      port: 5432" >> /home/airflow/.dbt/profiles.yml && \
    echo "      dbname: market_sentinel" >> /home/airflow/.dbt/profiles.yml && \
    echo "      schema: analytics" >> /home/airflow/.dbt/profiles.yml && \
    echo "      threads: 4" >> /home/airflow/.dbt/profiles.yml && \
    echo "  target: dev" >> /home/airflow/.dbt/profiles.yml
