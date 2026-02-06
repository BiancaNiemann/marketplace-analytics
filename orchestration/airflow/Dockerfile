FROM apache/airflow:2.10.2-python3.11

# Copy requirements first (better Docker caching)
COPY requirements.txt /requirements.txt

# Install Python dependencies AS airflow user
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt