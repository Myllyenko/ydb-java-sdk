package tech.ydb.core.impl.pool;

import java.util.Objects;

/**
 * @author Nikolay Perfilov
 */
public class EndpointRecord {
    private final String host;
    private final String hostAndPort;
    private final String locationDC;
    private final String authority;
    private final int port;
    private final int nodeId;

    public EndpointRecord(String host, int port, int nodeId, String locationDC, String authority) {
        this.host = Objects.requireNonNull(host);
        this.port = port;
        this.hostAndPort = host + ":" + port;
        this.nodeId = nodeId;
        this.locationDC = locationDC;
        if (authority != null && !authority.isEmpty()) {
            this.authority = authority;
        } else {
            this.authority = null;
        }
    }

    public EndpointRecord(String host, int port) {
        this(host, port, 0, null, null);
    }

    public String getHost() {
        return host;
    }

    public String getAuthority() {
        return authority;
    }

    public int getPort() {
        return port;
    }

    public String getHostAndPort() {
        return hostAndPort;
    }

    public int getNodeId() {
        return nodeId;
    }

    public String getLocation() {
        return locationDC;
    }

    @Override
    public String toString() {
        return "Endpoint{host=" + host + ", port=" + port + ", node=" + nodeId +
            ", location=" + locationDC + ", overrideAuthority=" + authority + "}";
    }
}
