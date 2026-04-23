package io.sentry.kqueue.example;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Example <strong>classic</strong> {@link KafkaConsumer} (normal consumer group, <strong>not</strong> KIP-932
 * share): subscribes by topic, polls, logs each value, commits that record's offset, then sleeps before the
 * next.
 *
 * <p>Env: same as {@link SlowShareConsumerApp} for bootstrap/topic/poll/sleep, plus {@code
 * KAFKA_GROUP_ID} default {@code kqueue-normal-example} (separate from share) and {@code
 * KAFKA_AUTO_OFFSET_RESET} (default {@code earliest} for local dev).
 */
public final class SlowNormalConsumerApp {

    public static void main(String[] args) {
        String bootstrap = "35.197.51.145:9092";
        String topic = "taskworker";
        String groupId = "start-from-earliest";
        int pollTimeoutMs = parseIntEnv("KAFKA_POLL_TIMEOUT_MS", 5000);
        long sleepMs = parseLongEnv("SLEEP_MS", 1000L);
        String autoOffset = firstNonBlank(System.getenv("KAFKA_AUTO_OFFSET_RESET"), "earliest");
        if (sleepMs < 0) {
            System.err.println("SLEEP_MS must be >= 0");
            return;
        }

        System.out.println(
                "SlowNormalConsumer: bootstrap=" + bootstrap + " group=" + groupId + " topic=" + topic
                        + " KAFKA_AUTO_OFFSET_RESET=" + autoOffset + " KAFKA_POLL_TIMEOUT_MS=" + pollTimeoutMs
                        + " SLEEP_MS=" + sleepMs);

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, firstNonBlank(System.getenv("KAFKA_MAX_POLL"), "32"));

        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(p)) {
            consumer.subscribe(List.of(topic));
            long count = 0L;
            while (true) {
                ConsumerRecords<byte[], byte[]> recs =
                        consumer.poll(Duration.ofMillis(Math.max(1, pollTimeoutMs)));
                if (recs == null || recs.isEmpty()) {
                    continue;
                }
                for (var r : recs) {
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
                    var tp = new TopicPartition(r.topic(), r.partition());
                    consumer.commitSync(
                            Collections.singletonMap(tp, new OffsetAndMetadata(r.offset() + 1)));
                    if (sleepMs > 0) {
                        Thread.sleep(sleepMs);
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
