FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .
COPY resources ./resources
COPY scripts ./scripts
COPY dashboard.html .
COPY dashboard_assets ./dashboard_assets
COPY config.example.yaml ./config.yaml
RUN chmod +x scripts/*.sh

COPY start.sh .
RUN chmod +x start.sh

VOLUME ["/app/buckets", "/app/state"]

ENV OMBRE_TRANSPORT=streamable-http
ENV OMBRE_BUCKETS_DIR=/app/buckets
ENV OMBRE_STATE_DIR=/app/state
ENV OMBRE_GATEWAY_ADMIN_URL=http://127.0.0.1:8010/api/config

EXPOSE 8000 8010

CMD ["./start.sh"]
