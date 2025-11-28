from arroyo.backends.kafka import KafkaProducer

def producer_factory(topic: str) -> KafkaProducer:
    config = {
        "bootstrap.servers": "127.0.0.1:9092",
        "compression.type": "lz4",
        "message.max.bytes": 50000000,  # 50MB
    }
    return KafkaProducer(config)
