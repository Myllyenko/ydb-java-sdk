package tech.ydb.topic.read.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import tech.ydb.proto.topic.YdbTopic.StreamReadMessage;
import tech.ydb.proto.topic.YdbTopic.StreamReadMessage.FromClient;
import tech.ydb.proto.topic.YdbTopic.StreamReadMessage.ReadResponse;
import tech.ydb.topic.description.Codec;
import tech.ydb.topic.description.CodecRegistry;
import tech.ydb.topic.read.PartitionSession;
import tech.ydb.topic.read.events.DataReceivedEvent;
import tech.ydb.topic.settings.ReaderSettings;
import tech.ydb.topic.settings.TopicReadSettings;
import tech.ydb.topic.utils.HideLoggers;
import tech.ydb.topic.utils.HideLoggersRule;

public class ReadSessionTest {
    private static final CodecRegistry REGISTRY = new CodecRegistry();

    @Rule
    public final HideLoggersRule hideLogger = new HideLoggersRule();

    private final ReadStreamMock stream = new ReadStreamMock();
    // decoding tasks are never executed, so the encoded messages are never ready
    private final Queue<Runnable> decodeTasks = new ConcurrentLinkedQueue<>();
    private final List<DataReceivedEvent> events = new ArrayList<>();

    private ReadSession createSession(long maxMemoryUsageBytes) {
        ReaderSettings settings = ReaderSettings.newBuilder()
                .addTopic(TopicReadSettings.newBuilder().setPath("/test-topic").build())
                .setConsumerName("consumer")
                .setMaxMemoryUsageBytes(maxMemoryUsageBytes)
                .build();
        ReadConfig config = new ReadConfig(REGISTRY, Runnable::run, decodeTasks::add, settings);
        return new ReadSession(
                "test",
                stream,
                FromClient.getDefaultInstance(),
                (control, event) -> events.add(event),
                config
        );
    }

    private static void startPartition(ReadSession session, long partitionSessionId) {
        session.onStartPartition(StreamReadMessage.StartPartitionSessionRequest.newBuilder()
                .setPartitionSession(StreamReadMessage.PartitionSession.newBuilder()
                        .setPath("/test-topic")
                        .setPartitionId(partitionSessionId)
                        .setPartitionSessionId(partitionSessionId)
                        .build())
                .build()
        ).confirm();
    }

    private static ReadResponse readResponse(long partitionSessionId, int codec, int messagesCount, int messageSize) {
        ReadResponse.Batch.Builder batch = ReadResponse.Batch.newBuilder().setCodec(codec);

        for (int offset = 0; offset < messagesCount; offset++) {
            batch.addMessageData(ReadResponse.MessageData.newBuilder()
                    .setOffset(offset)
                    .setUncompressedSize(messageSize)
                    .setData(ByteString.copyFrom(new byte[messageSize]))
                    .build()
            );
        }

        return ReadResponse.newBuilder()
                .setBytesSize((long) messagesCount * messageSize)
                .addPartitionData(ReadResponse.PartitionData.newBuilder()
                        .setPartitionSessionId(partitionSessionId)
                        .addBatches(batch.build())
                        .build()
                )
                .build();
    }

    @Test
    public void partitionStopReleasesMessagesTest() {
        ReadSession session = createSession(1000);
        ReadPartition partition = new ReadPartition("test", session, new PartitionSession(1, 1, "/test-topic"), 0);

        List<ReadResponse.Batch> batches = readResponse(1, Codec.GZIP, 3, 100).getPartitionData(0).getBatchesList();
        Assert.assertTrue(partition.addBatches(batches));
        // messages wait for decoding
        Assert.assertEquals(3, partition.getQueueSize());

        partition.stop();
        Assert.assertEquals(0, partition.getQueueSize());

        // stopped partition doesn't accept new messages
        Assert.assertFalse(partition.addBatches(batches));
        Assert.assertEquals(0, partition.getQueueSize());
    }

    @Test
    public void closeAllReleasesMessagesTest() {
        ReadSession session = createSession(1000);
        startPartition(session, 1);
        stream.assertSentMessagesCount(1);
        stream.assertLastMessage().isStartPartition(1);

        session.onRead(readResponse(1, Codec.GZIP, 3, 400));
        // 400 + 400 bytes are admitted for decoding, the last 400 bytes wait for the budget
        Assert.assertEquals(2, decodeTasks.size());
        Assert.assertEquals(1, session.getDecoder().getQueueSize());

        session.closeAll();
        Assert.assertEquals(0, session.getDecoder().getQueueSize());

        // buffer of the closed session is not tracked anymore, so releasing it doesn't request new data
        session.getBufferManager().releasePartition(1L);
        stream.assertSentMessagesCount(1);
    }

    @Test
    @HideLoggers({ReadSession.class})
    public void deferredCommitterReleasesStoppedPartitionsTest() {
        ReadSession session = createSession(1000);
        startPartition(session, 1);
        startPartition(session, 2);

        session.onRead(readResponse(1, Codec.RAW, 2, 10));
        session.onRead(readResponse(2, Codec.RAW, 2, 10));
        Assert.assertEquals(2, events.size());

        DeferredCommitterImpl committer = new DeferredCommitterImpl();
        events.forEach(committer::add);

        // committers of active partitions are kept
        committer.commit();
        Assert.assertEquals(2, committer.getCommittersCount());

        session.onClosePartition(1);
        committer.commit();
        Assert.assertEquals(1, committer.getCommittersCount());

        session.closeAll();
        committer.commit();
        Assert.assertEquals(0, committer.getCommittersCount());
    }
}
