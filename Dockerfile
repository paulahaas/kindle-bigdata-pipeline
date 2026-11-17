FROM python:3.11-slim-bookworm

# Spark precisa de Java (17 é suportado pelo Spark 3.5)
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kindle_pipeline ./kindle_pipeline
COPY scripts ./scripts

ENTRYPOINT ["python", "-m", "kindle_pipeline.run"]
