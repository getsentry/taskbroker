# Run from `clients/kafka-queue`: docker build -f java/queue-bridge/Dockerfile -t queue-bridge:dev .
# (Build JAR first; see java/queue-bridge/README.md)
FROM eclipse-temurin:17-jre
WORKDIR /app
COPY java/queue-bridge/build/libs/queue-bridge-all.jar /app/queue-bridge-all.jar
ENV GRPC_LISTEN_PORT=50060
EXPOSE 50060
USER nobody
ENTRYPOINT ["java", "-jar", "/app/queue-bridge-all.jar"]
