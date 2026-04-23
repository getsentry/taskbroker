package io.sentry.kqueue;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.util.logging.StreamHandler;

/**
 * gRPC + {@link ShareBridgeCore}. Required env: {@code KAFKA_BOOTSTRAP}, {@code KAFKA_GROUP_ID},
 * {@code KAFKA_TOPIC}. Optional: {@code GRPC_LISTEN_PORT} (default 50060).
 */
public final class BridgeServer {

    public static void main(String[] args) throws Exception {
        initJdkConsoleLogging();
        int port = Integer.parseInt(getEnv("GRPC_LISTEN_PORT", "50060"));
        ShareBridgeCore core = new ShareBridgeCore();
        Server server =
                ServerBuilder.forPort(port)
                        .addService(new BridgeService(core))
                        .build();
        server.start();
        String msg =
                "queue-bridge gRPC on "
                        + port
                        + " (KAFKA_BOOTSTRAP="
                        + getEnv("KAFKA_BOOTSTRAP", "")
                        + " KAFKA_GROUP_ID="
                        + getEnv("KAFKA_GROUP_ID", "")
                        + " KAFKA_TOPIC="
                        + getEnv("KAFKA_TOPIC", "")
                        + ")";
        System.out.println(msg);

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        server.shutdown().awaitTermination(10, TimeUnit.SECONDS);
                                    } catch (Exception ignored) {
                                    }
                                    try {
                                        core.close();
                                    } catch (Exception ignored) {
                                    }
                                }));
        server.awaitTermination();
    }

    private static String getEnv(String k, String d) {
        String v = System.getenv(k);
        if (v == null || v.isBlank()) {
            return d;
        }
        return v;
    }

    /**
     * Ensure {@link java.util.logging} INFO lines from the bridge reach the container. Uses {@link
     * System#out} (not the default JUL {@code ConsoleHandler} stderr) so GKE/Cloud Logging often classifies
     * these lines as INFO/notice instead of mapping stderr to ERROR.
     */
    private static void initJdkConsoleLogging() {
        Logger root = LogManager.getLogManager().getLogger("");
        if (root == null) {
            return;
        }
        root.setLevel(Level.INFO);
        for (var h : root.getHandlers()) {
            h.setLevel(Level.INFO);
        }
        if (root.getHandlers().length == 0) {
            StreamHandler h = new StreamHandler(System.out, new SimpleFormatter());
            h.setLevel(Level.INFO);
            root.addHandler(h);
        }
    }
}
