package tech.ydb.core.impl.pool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.net.SocketFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.core.grpc.BalancingSettings;

/**
 * @author Kirill Kurdyukov
 */
public class PriorityPicker {
    private static final Logger logger = LoggerFactory.getLogger(PriorityPicker.class);

    private static final int LOCALITY_SHIFT = 1000;
    private static final int DETECT_DC_NODE_SIZE = 3;
    private static final int DETECT_DC_TCP_PING_TIMEOUT_MS = 5000;
    /**
     * Nodes are probed one by one on the discovery thread, so with an unreachable datacenter the per node
     * timeout would be paid for every probed node of every datacenter. The whole measurement is budgeted
     * instead, which keeps it bounded no matter how many datacenters and nodes the cluster has.
     */
    private static final int DETECT_DC_TOTAL_TIMEOUT_MS = 5000;

    private final String preferredLocation;

    private PriorityPicker(String location) {
        this.preferredLocation = location;
    }

    public int getEndpointPriority(String location) {
        if (preferredLocation == null || preferredLocation.equalsIgnoreCase(location)) {
            return 0;
        }

        return LOCALITY_SHIFT;
    }

    public static PriorityPicker from(BalancingSettings settings, String selfLocation, List<EndpointRecord> endpoints) {
        switch (settings.getPolicy()) {
            case USE_ALL_NODES:
                return new PriorityPicker(null);
            case USE_PREFERABLE_LOCATION:
                return new PriorityPicker(getLocationFromConfig(settings.getPreferableLocation(), selfLocation));
            case USE_DETECT_LOCAL_DC:
                return new PriorityPicker(detectLocalDC(endpoints, Ticker.systemTicker()));
            default:
                throw new RuntimeException("Not implemented balancing policy: " + settings.getPolicy().name());
        }
    }

    @VisibleForTesting
    static String getLocationFromConfig(String preferable, String selfLocation) {
        if (preferable != null && !preferable.isEmpty()) {
            return preferable;
        }
        if (selfLocation != null && !selfLocation.isEmpty()) {
            return selfLocation;
        }
        return null;
    }

    @VisibleForTesting
    static String detectLocalDC(List<EndpointRecord> endpoints, Ticker ticker) {
        Map<String, List<EndpointRecord>> dcLocationToNodes = endpoints
                .stream()
                .collect(Collectors.groupingBy(EndpointRecord::getLocation));

        if (dcLocationToNodes.size() < 2) {
            return null;
        }

        long minPing = Long.MAX_VALUE;
        String localDC = null;
        // wall clock deadline for the whole detection, independent of the ticker used for measuring
        long deadlineNanos = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(DETECT_DC_TOTAL_TIMEOUT_MS);

        for (Map.Entry<String, List<EndpointRecord>> entry : dcLocationToNodes.entrySet()) {
            String dc = entry.getKey();
            List<EndpointRecord> nodes = entry.getValue();

            assert !nodes.isEmpty();

            Collections.shuffle(nodes);

            int nodeSize = Math.min(nodes.size(), DETECT_DC_NODE_SIZE);
            long tcpPing = 0;

            for (EndpointRecord node : nodes.subList(0, nodeSize)) {
                InetSocketAddress address = new InetSocketAddress(node.getHost(), node.getPort());
                long currentPing = tcpPing(address, ticker, connectTimeoutMs(deadlineNanos));
                logger.debug("Address: {}, port: {}, nanos ping: {}", node.getHost(), node.getPort(), currentPing);
                tcpPing += currentPing;
            }

            tcpPing /= nodeSize;

            if (minPing > tcpPing) {
                minPing = tcpPing;
                localDC = dc;
            }
        }

        return localDC;
    }

    private static int connectTimeoutMs(long deadlineNanos) {
        long leftMs = TimeUnit.NANOSECONDS.toMillis(deadlineNanos - System.nanoTime());
        // Socket.connect treats a zero timeout as an infinite one, so never pass it
        return (int) Math.max(1, Math.min(leftMs, DETECT_DC_TCP_PING_TIMEOUT_MS));
    }

    private static long tcpPing(InetSocketAddress socketAddress, Ticker ticker, int timeoutMs) {
        try (Socket socket = SocketFactory.getDefault().createSocket()) {
            final long startConnection = ticker.read();
            socket.connect(socketAddress, timeoutMs);
            final long stopConnection = ticker.read();
            return stopConnection - startConnection;
        } catch (IOException e) {
            return Long.MAX_VALUE;
        }
    }
}
