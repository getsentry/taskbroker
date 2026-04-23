# Run from `clients/kafka-queue`: docker build -f python/Dockerfile -t kqueue-worker:dev .
FROM python:3.12-slim
WORKDIR /w
RUN pip install --no-cache-dir -U pip setuptools wheel
COPY python/ /w/python/
RUN pip install --no-cache-dir /w/python/
ENV PYTHONUNBUFFERED=1
ENV QUEUE_BRIDGE_ADDR=queue-bridge:50060
ENTRYPOINT ["kqueue", "worker"]
CMD ["--bridge", "queue-bridge:50060"]
