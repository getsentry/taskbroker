package io.sentry.kqueue;

import static java.util.Objects.requireNonNull;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.AcknowledgeType;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaShareConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

/**
 * Buffers one Kafka share-group poll at a time; each record is handed to Python with a delivery id. Explicit
 * ack before the next share-group poll re-enqueues; multiple records from one batch may be in flight in any
 * order, which is valid for explicit share acknowledgement.
 */
public final class ShareBridgeCore implements AutoCloseable {
    private static final Logger log = Logger.getLogger(ShareBridgeCore.class.getName());

    // sentry_protos TaskActivationStatus: align with your generated Python constants.
    public static final int ST_COMPLETE = 1;
    public static final int ST_FAILURE = 2;
    public static final int ST_RETRY = 3;

    private final KafkaShareConsumer<byte[], byte[]> consumer;
    private final String bootstrap;
    private final String groupId;
    private final String topic;
    private final ArrayDeque<Handoff> handoff = new ArrayDeque<>();
    private final Map<String, ConsumerRecord<byte[], byte[]>> inFlight = new HashMap<>();
    private final int emptyLogInterval;
    private long totalPolls;
    private long totalDeliveries;
    private long totalCompletes;
    /** Count of kafka polls that returned no new records to hand off. */
    private long emptyPollCount;

    public ShareBridgeCore() {
        this.bootstrap = requireNonNull(env("KAFKA_BOOTSTRAP", null));
        this.groupId = requireNonNull(env("KAFKA_GROUP_ID", null));
        this.topic = requireNonNull(env("KAFKA_TOPIC", null));
        this.emptyLogInterval = parseEmptyLogInterval();

        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        // Share group (KIP-932) rejects enable.auto.commit and auto.offset.reset — not valid here.
        p.put("share.acknowledgement.mode", "explicit");
        p.put("share.acquire.mode", "record_limit");
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, env("KAFKA_MAX_POLL", "32"));

        this.consumer = new KafkaShareConsumer<>(p);
        this.consumer.subscribe(List.of(topic));
        log.log(
                Level.INFO,
                "ShareBridgeCore: KafkaShareConsumer created bootstrap={0} group={1} topic={2} maxPollRecords={3} emptyLogEveryN={4}",
                new Object[] {bootstrap, groupId, topic, p.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), emptyLogInterval});
    }

    public synchronized PollResult poll(int timeoutMs) {
        totalPolls++;
        if (!handoff.isEmpty()) {
            return deliverHandoff("buffered");
        }
        return pollKafka(Duration.ofMillis(Math.max(1, timeoutMs)));
    }

    private PollResult deliverHandoff(String source) {
        Handoff h = handoff.removeFirst();
        inFlight.put(h.deliveryId(), h.rec());
        totalDeliveries++;
        var rec = h.rec();
        int valLen = rec.value() == null ? 0 : rec.value().length;
        log.log(
                Level.INFO,
                "ShareBridgeCore: deliver source={0} deliveryId={1} topic={2} partition={3} offset={4} valueBytes={5} inFlight={6} handoffRemaining={7} totalPolls={8} totalDeliveries={9}",
                new Object[] {
                    source,
                    h.deliveryId(),
                    rec.topic(),
                    rec.partition(),
                    rec.offset(),
                    valLen,
                    inFlight.size(),
                    handoff.size(),
                    totalPolls,
                    totalDeliveries
                });
        return new PollResult(
                false,
                h.deliveryId(),
                h.value(),
                totalPolls,
                totalDeliveries);
    }

    private PollResult pollKafka(Duration d) {
        ConsumerRecords<byte[], byte[]> recs;
        if (d.isZero()) d = Duration.ofMillis(1);
        recs = consumer.poll(d);
        if (recs == null || recs.isEmpty()) {
            logEmptySample("ShareBridgeCore: kafka consumer.poll() returned isEmpty (duration=" + d + " inFlight=" + inFlight.size() + " handoff=" + handoff.size() + ")");
            return new PollResult(true, null, null, totalPolls, totalDeliveresSafe());
        }
        int nRecords = 0;
        for (TopicPartition tp : recs.partitions()) {
            nRecords += recs.records(tp).size();
        }
        log.log(
                Level.INFO,
                "ShareBridgeCore: kafka batch partitions={0} totalRecordsInBatch={1} inFlight={2} (before add to handoff)",
                new Object[] {recs.partitions().size(), nRecords, inFlight.size()});
        for (TopicPartition tp : recs.partitions()) {
            for (ConsumerRecord<byte[], byte[]> r : recs.records(tp)) {
                String id = UUID.randomUUID().toString();
                handoff.addLast(new Handoff(id, r));
            }
        }
        if (handoff.isEmpty()) {
            log.log(
                    Level.WARNING,
                    "ShareBridgeCore: non-empty ConsumerRecords but handoff queue empty (unexpected) totalPolls={0}",
                    totalPolls);
            return new PollResult(true, null, null, totalPolls, totalDeliveresSafe());
        }
        return deliverHandoff("kafka");
    }

    private long totalDeliveresSafe() {
        return totalDeliveries;
    }

    public synchronized CompleteResult complete(CompleteIn in) {
        log.log(
                Level.INFO,
                "ShareBridgeCore: complete start deliveryId={0} taskId={1} activationStatus={2} fetchNext={3} pollTimeoutMs={4} inFlightBefore={5} handoff={6}",
                new Object[] {
                    in.deliveryId(),
                    in.taskId() == null ? "" : in.taskId(),
                    in.activationStatus(),
                    in.fetchNext(),
                    in.timeoutMs(),
                    inFlight.size(),
                    handoff.size()
                });
        ConsumerRecord<byte[], byte[]> r = inFlight.remove(in.deliveryId());
        if (r == null) {
            log.log(
                    Level.SEVERE,
                    "ShareBridgeCore: complete unknown delivery_id (not in inFlight) deliveryId={0} inFlight keys sample size={1}",
                    new Object[] {in.deliveryId(), inFlight.size()});
            throw new IllegalStateException("unknown delivery_id: " + in.deliveryId());
        }
        AcknowledgeType t = toAckType(in.activationStatus());
        log.log(
                Level.INFO,
                "ShareBridgeCore: acknowledge+commit deliveryId={0} partition={1} offset={2} ackType={3}",
                new Object[] {in.deliveryId(), r.partition(), r.offset(), t});
        consumer.acknowledge(r, t);
        consumer.commitSync();
        totalCompletes++;
        if (!in.fetchNext()) {
            log.log(
                    Level.INFO,
                    "ShareBridgeCore: complete end (no fetch_next) totalCompletes={0} inFlight={1} handoff={2}",
                    new Object[] {totalCompletes, inFlight.size(), handoff.size()});
            return new CompleteResult(false, null, null, totalCompletes);
        }
        log.log(
                Level.INFO,
                "ShareBridgeCore: complete with fetchNext calling poll({0} ms)", in.timeoutMs());
        PollResult n = poll(in.timeoutMs);
        if (n.empty()) {
            log.log(
                    Level.INFO,
                    "ShareBridgeCore: complete fetch_next path returned empty totalCompletes={0} inFlight={1} handoff={2}",
                    new Object[] {totalCompletes, inFlight.size(), handoff.size()});
            return new CompleteResult(false, null, null, totalCompletes);
        }
        log.log(
                Level.INFO,
                "ShareBridgeCore: complete fetch_next got next deliveryId={0} valueBytes={1}",
                new Object[] {n.deliveryId(), n.payload() == null ? 0 : n.payload().length});
        return new CompleteResult(
                true,
                n.deliveryId(),
                n.payload(),
                totalCompletes);
    }

    private static AcknowledgeType toAckType(int s) {
        if (s == ST_COMPLETE) return AcknowledgeType.ACCEPT;
        if (s == ST_RETRY) return AcknowledgeType.RELEASE;
        if (s == ST_FAILURE) return AcknowledgeType.REJECT;
        // default: re-eligible
        return AcknowledgeType.RELEASE;
    }

    @Override
    public synchronized void close() {
        log.log(
                Level.INFO,
                "ShareBridgeCore: close() totalPolls={0} totalDeliveries={1} totalCompletes={2} inFlight={3} handoff={4}",
                new Object[] {totalPolls, totalDeliveries, totalCompletes, inFlight.size(), handoff.size()});
        consumer.close(Duration.ofSeconds(5));
    }

    /**
     * Throttle "empty poll" log lines. Env {@code KQUEUE_BRIDGE_LOG_EMPTY_EVERY}: {@code 0} = never, {@code
     * 1} = every empty poll, default {@code 20}.
     */
    private static int parseEmptyLogInterval() {
        String e = System.getenv("KQUEUE_BRIDGE_LOG_EMPTY_EVERY");
        if (e == null || e.isBlank()) {
            return 20;
        }
        try {
            return Integer.parseInt(e.trim());
        } catch (NumberFormatException ex) {
            return 20;
        }
    }

    private void logEmptySample(String message) {
        if (emptyLogInterval <= 0) {
            return;
        }
        emptyPollCount++;
        if (emptyPollCount == 1 || (emptyPollCount % emptyLogInterval) == 0) {
            log.log(
                    Level.INFO,
                    message + " emptySampleCount=" + emptyPollCount + " totalPolls=" + totalPolls);
        }
    }

    private static String env(String k, String def) {
        String v = System.getenv(k);
        return v == null || v.isBlank() ? def : v;
    }

    private record Handoff(String deliveryId, ConsumerRecord<byte[], byte[]> rec) {
        byte[] value() {
            if (rec.value() == null) {
                return new byte[0];
            }
            return rec.value();
        }
    }

    public record PollResult(
            boolean empty, String deliveryId, byte[] payload, long totalPolls, long totalDeliveries) {}

    public record CompleteIn(
            String deliveryId, String taskId, int activationStatus, boolean fetchNext, int timeoutMs) {}

    public record CompleteResult(
            boolean hasNext, String nextDeliveryId, byte[] nextPayload, long totalCompletes) {}
}
