package tech.ydb.topic.read.impl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import tech.ydb.topic.TopicRpc;
import tech.ydb.topic.description.CodecRegistry;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.ReceiveSettings;
import tech.ydb.topic.settings.TopicReadSettings;

/**
 *
 * @author Aleksandr Gorshenin
 */
public class SyncReaderImplTest {
    private final ExecutorService executor = Executors.newFixedThreadPool(2);

    @After
    public void shutdown() {
        executor.shutdownNow();
    }

    private static SyncReaderImpl createReader() {
        TopicRpc rpc = Mockito.mock(TopicRpc.class);
        ReaderSettings settings = ReaderSettings.newBuilder()
                .setConsumerName("consumer")
                .addTopic(TopicReadSettings.newBuilder().setPath("/topic").build())
                .build();

        return new SyncReaderImpl(rpc, settings, new CodecRegistry());
    }

    private static DataReceivedEvent eventWithMessages(int count) {
        Message[] messages = new Message[count];
        for (int idx = 0; idx < count; idx += 1) {
            messages[idx] = Mockito.mock(Message.class);
        }

        DataReceivedEvent event = Mockito.mock(DataReceivedEvent.class);
        Mockito.when(event.getMessages()).thenReturn(Arrays.asList(messages));
        return event;
    }

    /**
     * A single batch can feed several blocked receive() calls, so every waiter has to be woken when it
     * arrives. Waking only one leaves the others parked on the condition: they pick their message up
     * only when their own wait expires, or report "no messages" if it expires first.
     */
    @Test(timeout = 120_000)
    public void everyWaiterIsWokenByANewBatch() throws Exception {
        // a wait long enough that a missed notification is visible as latency instead of as a null
        final long receiveTimeoutSeconds = 30;
        final long allowedLatencyMillis = 5_000;

        SyncReaderImpl reader = createReader();

        final int readerCount = 2;
        CountDownLatch waiting = new CountDownLatch(readerCount);
        List<CompletableFuture<Message>> received = Arrays.asList(
                new CompletableFuture<>(), new CompletableFuture<>()
        );

        ReceiveSettings receiveSettings = ReceiveSettings.newBuilder()
                .setTimeout(receiveTimeoutSeconds, TimeUnit.SECONDS)
                .build();

        for (int idx = 0; idx < readerCount; idx += 1) {
            CompletableFuture<Message> target = received.get(idx);
            executor.submit(() -> {
                waiting.countDown();
                try {
                    target.complete(reader.receiveInternal(
                            receiveSettings, receiveTimeoutSeconds, TimeUnit.SECONDS
                    ));
                } catch (Throwable th) {
                    target.completeExceptionally(th);
                }
                return null;
            });
        }

        Assert.assertTrue(waiting.await(10, TimeUnit.SECONDS));
        // give both readers a chance to reach the await inside receiveInternal
        Thread.sleep(500);

        // one batch with a message for each waiting reader
        long postedAt = System.nanoTime();
        reader.handleDataReceivedEvent(eventWithMessages(readerCount));

        for (CompletableFuture<Message> future : received) {
            Assert.assertNotNull(future.get(receiveTimeoutSeconds + 30, TimeUnit.SECONDS));
        }

        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - postedAt);
        Assert.assertTrue("waiting readers were served only after " + elapsedMillis
                + " ms, so at least one of them was not notified", elapsedMillis < allowedLatencyMillis);
    }

    @Test(timeout = 30_000)
    public void messagesOfABatchAreReadInOrder() throws Exception {
        SyncReaderImpl reader = createReader();
        DataReceivedEvent event = eventWithMessages(3);

        CompletableFuture<Void> batchRead = reader.handleDataReceivedEvent(event);
        Assert.assertFalse(batchRead.isDone());

        ReceiveSettings receiveSettings = ReceiveSettings.newBuilder()
                .setTimeout(5, TimeUnit.SECONDS)
                .build();

        for (Message expected : event.getMessages()) {
            Assert.assertSame(expected, reader.receiveInternal(receiveSettings, 5, TimeUnit.SECONDS));
        }

        // the batch future completes only when the whole batch has been handed to the user
        Assert.assertTrue(batchRead.isDone());
        Assert.assertNull(reader.receiveInternal(receiveSettings, 10, TimeUnit.MILLISECONDS));
    }
}
