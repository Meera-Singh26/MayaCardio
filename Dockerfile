FROM python:3.11-slim
WORKDIR /app
COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["sh", "-c", "python -m pipeline cloud --port ${PORT:-10000} --db /tmp/cloud.db --api-key $PIPELINE_API_KEY"]
