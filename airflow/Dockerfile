FROM apache/airflow:2.8.1

# Switch to airflow user for pip installs
USER airflow

# Install Amazon provider
RUN pip install --no-cache-dir apache-airflow-providers-amazon

# Switch back to airflow (optional, it's already the user)
USER airflow

