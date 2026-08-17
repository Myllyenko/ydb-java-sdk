package tech.ydb.core.impl.pool;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import com.google.common.base.Ticker;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import tech.ydb.core.grpc.BalancingSettings;

/**
 * @author Kirill
 */
public class PriorityPickerTest {

    @Test
    public void randomEvaluatorTest() {
        PriorityPicker picker = PriorityPicker.from(BalancingSettings.defaultInstance(), null, null);
        Assert.assertEquals(0, picker.getEndpointPriority("DC1"));
        Assert.assertEquals(0, picker.getEndpointPriority("DC2"));
        Assert.assertEquals(0, picker.getEndpointPriority("DC3"));
    }


    @Test
    public void fixedLocalDcTest() {
        PriorityPicker ignoreSelfLocation = PriorityPicker.from(BalancingSettings.defaultInstance(), "DC1", null);
        Assert.assertEquals(0, ignoreSelfLocation.getEndpointPriority("dC1"));
        Assert.assertEquals(0, ignoreSelfLocation.getEndpointPriority("Dc2"));
        Assert.assertEquals(0, ignoreSelfLocation.getEndpointPriority("Dc3"));

        PriorityPicker useSelfLocation = PriorityPicker.from(BalancingSettings.fromLocation(""), "DC1", null);
        Assert.assertEquals(0, useSelfLocation.getEndpointPriority("dC1"));
        Assert.assertEquals(1000, useSelfLocation.getEndpointPriority("Dc2"));
        Assert.assertEquals(1000, useSelfLocation.getEndpointPriority("Dc3"));

        PriorityPicker useLocalDC = PriorityPicker.from(BalancingSettings.fromLocation("DC2"), "DC1", null);
        Assert.assertEquals(1000, useLocalDC.getEndpointPriority("dC1"));
        Assert.assertEquals(0, useLocalDC.getEndpointPriority("Dc2"));
        Assert.assertEquals(1000, useLocalDC.getEndpointPriority("Dc3"));
    }

    @Test
    public void detectLocalDCFallbackTest() {
        List<EndpointRecord> single = Collections.singletonList(new EndpointRecord("localhost", 8080, 0, "DC1", null));
        PriorityPicker ignoreSelfLocation = PriorityPicker.from(BalancingSettings.detectLocalDs(), "DC1", single);

        Assert.assertEquals(0, ignoreSelfLocation.getEndpointPriority("DC1"));
        Assert.assertEquals(0, ignoreSelfLocation.getEndpointPriority("DC2"));
        Assert.assertEquals(0, ignoreSelfLocation.getEndpointPriority("DC3"));
    }

    /**
     * Nodes are probed sequentially on the discovery thread. With an unreachable datacenter every probe
     * pays the connect timeout, so the detection has to be budgeted as a whole instead of per node.
     */
    @Test(timeout = 120_000)
    public void detectLocalDCIsBudgetedTest() throws IOException {
        final int datacenters = 6;
        final int nodesPerDatacenter = 3;

        Socket socket = Mockito.mock(Socket.class);
        SocketFactory socketFactory = Mockito.mock(SocketFactory.class);
        Mockito.when(socketFactory.createSocket()).thenReturn(socket);

        // an unreachable node: connect blocks for the whole timeout it was given and then fails
        Mockito.doAnswer(invocation -> {
            Thread.sleep(invocation.getArgument(1, Integer.class));
            throw new IOException("unreachable");
        }).when(socket).connect(Mockito.any(), Mockito.anyInt());

        List<EndpointRecord> records = new ArrayList<>();
        for (int dc = 0; dc < datacenters; dc += 1) {
            for (int node = 0; node < nodesPerDatacenter; node += 1) {
                records.add(new EndpointRecord("10.0." + dc + "." + node, 2136, 0, "DC" + dc, null));
            }
        }

        try (MockedStatic<SocketFactory> socketFactoryMock = Mockito.mockStatic(SocketFactory.class)) {
            socketFactoryMock.when(SocketFactory::getDefault).thenReturn(socketFactory);

            long startedAt = System.nanoTime();
            PriorityPicker.detectLocalDC(records, Ticker.systemTicker());
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

            // unbudgeted this would be 18 probes of 5 seconds each
            Assert.assertTrue("local dc detection took " + elapsedMillis + " ms", elapsedMillis < 30_000);
        }
    }

    @Test
    public void detectLocalDCTest() {
        TestTicker testTicker = new TestTicker(
                9, 15,
                16, 50,
                51, 74,
                75, 77,
                78, 82,
                83, 125
        );

        try (ServerSocket serverSocket = ServerSocketFactory.getDefault().createServerSocket(0)) {
            Assert.assertFalse(serverSocket.isClosed());
            final int port = serverSocket.getLocalPort();

            List<EndpointRecord> records = Stream.of("DC1", "DC1", "DC2", "DC2", "DC2", "DC3")
                    .map(location -> new EndpointRecord("localhost", port, 1, location, null))
                    .collect(Collectors.toList());

            String localDC = PriorityPicker.detectLocalDC(records, testTicker);
            Assert.assertEquals("DC1", localDC);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
