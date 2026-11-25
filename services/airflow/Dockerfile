FROM apache/airflow:2.7.1

USER root

# Install docker CLI to run docker commands from inside container
RUN apt-get update && apt-get install -y docker.io && rm -rf /var/lib/apt/lists/*

# Copy custom entrypoint and make it executable
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the correct PYTHONPATH for airflow
ENV PATH="/home/airflow/.local/bin:$PATH"
ENV PYTHONPATH="/home/airflow/.local/lib/python3.8/site-packages:$PYTHONPATH"

ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
