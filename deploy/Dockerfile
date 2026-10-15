# Cloud service image (free tiers: Render, Fly.io, Railway, Oracle Always Free ARM VM ...).
FROM python:3.12-slim
WORKDIR /app
COPY pipeline /app/pipeline
ENV PYTHONUNBUFFERED=1 PIPELINE_CLOUD_DB=/data/cloud.db PORT=8080
VOLUME /data
EXPOSE 8080
CMD ["python3", "-m", "pipeline", "cloud"]
