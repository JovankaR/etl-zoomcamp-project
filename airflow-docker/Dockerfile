FROM apache/airflow:2.7.1-python3.11

# Switch to root to install system dependencies
USER root

# Install system dependencies required for Airflow, GCP, and data processing
RUN apt-get update && \
	apt-get install -y --no-install-recommends \
		gcc \
		g++ \
		libpq-dev \
		curl \
		git \
	&& apt-get clean && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# (Optional) If you want to build a self-contained image, uncomment below to copy DAGs/scripts/spark_jobs
# COPY dags/ /opt/airflow/dags/
# COPY scripts/ /opt/airflow/scripts/
# COPY spark_jobs/ /opt/airflow/spark_jobs/

# (Optional) Set environment variables here if needed
# ENV GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/secrets/google_credentials.json

# (Optional) Set permissions if needed
# RUN chown -R airflow: /opt/airflow