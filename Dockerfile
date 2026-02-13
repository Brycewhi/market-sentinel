FROM apache/airflow:2.7.0-python3.10

USER airflow

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install all packages from requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
