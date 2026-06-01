FROM python:3.11-slim

# PySpark requires a JVM. default-jre-headless installs OpenJDK 17 on
# Debian Bookworm and creates /usr/lib/jvm/default-java — a stable symlink
# that resolves correctly on both amd64 (CI/prod) and arm64 (M-series Macs).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# Copy and install dependencies first — this layer is cached unless
# requirements.txt changes, keeping rebuilds fast.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code and config
COPY src/ ./src/
COPY Contract_rules.yaml .
COPY run.sh .
RUN chmod +x run.sh

# Input and output data are mounted at runtime (see docker-compose.yml).
# Declaring the volume here documents the interface — anyone reading this
# Dockerfile knows immediately that /app/data is externally supplied.
VOLUME ["/app/data"]

CMD ["bash", "run.sh"]
