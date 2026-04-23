package io.sentry.kqueue.example;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Example {@link KafkaShareConsumer}: logs each value, {@link KafkaShareConsumer#acknowledge} + {@link
 * KafkaShareConsumer#commitSync} as in {@code queue-bridge}, then sleeps a fixed delay before handling the
 * next record (in batch order, across partitions).
 *
 * <p>Env: {@code KAFKA_BOOTSTRAP}, {@code KAFKA_TOPIC}, optional {@code KAFKA_GROUP_ID} (default
 * {@code kqueue-slow-example} so you do not collide with the bridge), {@code SLEEP_MS} (default {@code
 * 1000}), {@code KAFKA_POLL_TIMEOUT_MS} (default {@code 5000}), {@code KAFKA_MAX_POLL} (default {@code
 * 32}).
 */
public final class SlowShareConsumerApp {

    public static void main(String[] args) {
        String bootstrap = "35.197.51.145:9092";
        String topic = "taskworker";
        String groupId = "start-from-earliest";
        int pollTimeoutMs = parseIntEnv("KAFKA_POLL_TIMEOUT_MS", 5000);
        long sleepMs = parseLongEnv("SLEEP_MS", 1000L);
        if (sleepMs < 0) {
            System.err.println("SLEEP_MS must be >= 0");
            return;
        }

        System.out.println(
                "SlowShareConsumer: bootstrap=" + bootstrap + " group=" + groupId + " topic=" + topic
                        + " KAFKA_POLL_TIMEOUT_MS=" + pollTimeoutMs + " SLEEP_MS=" + sleepMs);

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put("share.acknowledgement.mode", "explicit");
        p.put("share.acquire.mode", "record_limit");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, firstNonBlank(System.getenv("KAFKA_MAX_POLL"), "32"));

        try (KafkaShareConsumer<byte[], byte[]> consumer = new KafkaShareConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            long count = 0L;
            while (true) {
                ConsumerRecords<byte[], byte[]> recs =
                        consumer.poll(Duration.ofMillis(Math.max(1, pollTimeoutMs)));
                if (recs == null || recs.isEmpty()) {
                    continue;
                }
                for (TopicPartition tp : recs.partitions()) {
                    for (ConsumerRecord<byte[], byte[]> r : recs.records(tp)) {
                        count++;
                        int vlen = r.value() == null ? 0 : r.value().length;
                        System.out.println(
                                "message n="
                                        + count
                                        + " topic="
                                        + r.topic()
                                        + " partition="
                                        + r.partition()
                                        + " offset="
                                        + r.offset()
                                        + " valueBytes="
                                        + vlen);
                        consumer.acknowledge(r, AcknowledgeType.ACCEPT);
                        consumer.commitSync();
                        if (sleepMs > 0) {
                            Thread.sleep(sleepMs);
                        }
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("interrupted, exiting");
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static int parseIntEnv(String key, int d) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            return d;
        }
        return Integer.parseInt(v.trim());
    }

    private static long parseLongEnv(String key, long d) {
        String v = System.getenv(key);
        if (v == null || v.isBlank()) {
            return d;
        }
        return Long.parseLong(v.trim());
    }

    private static String firstNonBlank(String v, String d) {
        if (v == null || v.isBlank()) {
            return d;
        }
        return v;
    }
}
