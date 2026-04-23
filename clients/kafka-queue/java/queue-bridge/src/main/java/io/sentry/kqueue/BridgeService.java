package io.sentry.kqueue;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import java.util.logging.Logger;

/**
 * gRPC service → {@link ShareBridgeCore} (synchronized; safe under concurrent Netty I/O if every RPC is
 * serialized in core—here core methods are {@code synchronized} on the same instance).
 */
public final class BridgeService extends QueueBridgeGrpc.QueueBridgeImplBase {
    private static final String BRIDGE_HOST = "queue-bridge";
    private static final Logger log = Logger.getLogger(BridgeService.class.getName());

    private final ShareBridgeCore core;

    public BridgeService(ShareBridgeCore core) {
        this.core = core;
    }

    @Override
    public void poll(
            io.sentry.kqueue.PollRequest request, StreamObserver<io.sentry.kqueue.PollResponse> o) {
        try {
            int ms = request.getPollTimeoutMs() > 0 ? request.getPollTimeoutMs() : 5000;
            log.log(
                    java.util.logging.Level.INFO,
                    "BridgeService: gRPC Poll call pollTimeoutMs={0,number,#} ms",
                    new Object[] {ms});
            ShareBridgeCore.PollResult r = core.poll(ms);
            int payloadBytes = (r.empty() || r.payload() == null) ? 0 : r.payload().length;
            log.log(
                    java.util.logging.Level.INFO,
                    "BridgeService: gRPC Poll done empty={0} deliveryId={1} totalPolls={2} totalDeliveries={3} responsePayloadBytes={4,number,#}",
                    new Object[] {
                        r.empty(),
                        r.deliveryId() == null ? "" : r.deliveryId(),
                        r.totalPolls(),
                        r.totalDeliveries(),
                        payloadBytes
                    });
            var b = io.sentry.kqueue.PollResponse.newBuilder();
            b.setEmpty(r.empty());
            b.setTotalPolls(r.totalPolls());
            b.setTotalDeliveries(r.totalDeliveries());
            b.setBridgeHost(BRIDGE_HOST);
            if (!r.empty() && r.deliveryId() != null) {
                b.setDeliveryId(r.deliveryId());
                if (r.payload() != null) {
                    b.setPayload(ByteString.copyFrom(r.payload()));
                }
            }
            o.onNext(b.build());
            o.onCompleted();
        } catch (Exception e) {
            log.log(java.util.logging.Level.SEVERE, "poll failed", e);
            o.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage() != null ? e.getMessage() : "poll failed")
                            .asRuntimeException());
        }
    }

    @Override
    public void complete(
            io.sentry.kqueue.CompleteRequest request,
            StreamObserver<io.sentry.kqueue.CompleteResponse> o) {
        try {
            int pollMs = request.getPollTimeoutMs() > 0 ? request.getPollTimeoutMs() : 5000;
            log.log(
                    java.util.logging.Level.INFO,
                    "BridgeService: gRPC Complete call deliveryId={0} taskId={1} activationStatus={2} fetchNext={3} pollTimeoutMs={4,number,#}",
                    new Object[] {
                        request.getDeliveryId(),
                        request.getTaskId() == null ? "" : request.getTaskId(),
                        request.getActivationStatus(),
                        request.getFetchNext(),
                        pollMs
                    });
            var in =
                    new ShareBridgeCore.CompleteIn(
                            request.getDeliveryId(),
                            request.getTaskId(),
                            request.getActivationStatus(),
                            request.getFetchNext(),
                            pollMs);
            ShareBridgeCore.CompleteResult r = core.complete(in);
            int nextBytes =
                    (r.hasNext() && r.nextPayload() != null) ? r.nextPayload().length : 0;
            log.log(
                    java.util.logging.Level.INFO,
                    "BridgeService: gRPC Complete done hasNext={0} nextDeliveryId={1} nextPayloadBytes={2,number,#} totalCompletes={3,number,#}",
                    new Object[] {
                        r.hasNext(),
                        r.nextDeliveryId() == null ? "" : r.nextDeliveryId(),
                        nextBytes,
                        r.totalCompletes()
                    });
            var b = io.sentry.kqueue.CompleteResponse.newBuilder();
            b.setHasNext(r.hasNext());
            b.setTotalCompletes(r.totalCompletes());
            b.setBridgeHost(BRIDGE_HOST);
            if (r.hasNext() && r.nextDeliveryId() != null) {
                b.setNextDeliveryId(r.nextDeliveryId());
                if (r.nextPayload() != null) {
                    b.setNextPayload(ByteString.copyFrom(r.nextPayload()));
                }
            }
            o.onNext(b.build());
            o.onCompleted();
        } catch (Exception e) {
            log.log(java.util.logging.Level.SEVERE, "complete failed", e);
            o.onError(
                    Status.INTERNAL
                            .withDescription(e.getMessage() != null ? e.getMessage() : "complete failed")
                            .asRuntimeException());
        }
    }

    @Override
    public void check(
            io.sentry.kqueue.CheckRequest request, StreamObserver<io.sentry.kqueue.CheckResponse> o) {
        var b = io.sentry.kqueue.CheckResponse.newBuilder();
        b.setReady(true);
        b.setDetail("ok");
        o.onNext(b.build());
        o.onCompleted();
    }
}
