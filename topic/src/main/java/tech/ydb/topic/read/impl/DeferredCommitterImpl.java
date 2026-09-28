package tech.ydb.topic.read.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tech.ydb.topic.read.DeferredCommitter;
import tech.ydb.topic.read.Message;
import tech.ydb.topic.read.MessageCommitter;
import tech.ydb.topic.read.PartitionSession;
import tech.ydb.topic.read.events.DataReceivedEvent;

/**
 * @author Nikolay Perfilov
 */
public class DeferredCommitterImpl implements DeferredCommitter {
    private static final Logger logger = LoggerFactory.getLogger(DeferredCommitterImpl.class);

    private final Map<MessageCommitter, DisjointOffsetRangeSet> rangesBySession = new ConcurrentHashMap<>();

    private RuntimeException wrapExceptionWithSession(PartitionSession session, RuntimeException ex) {
        String msg = "Error adding new offset range to DeferredCommitter for " + session + ": " + ex.getMessage();
        logger.error(msg);
        return new RuntimeException(msg, ex);
    }

    @Override
    public void add(Message message) {
        MessageCommitter committer = message.getCommitter();
        DisjointOffsetRangeSet range = rangesBySession.computeIfAbsent(committer, s -> new DisjointOffsetRangeSet());
        try {
            range.add(message.getRangeToCommit());
        } catch (RuntimeException exception) {
            throw wrapExceptionWithSession(message.getPartitionSession(), exception);
        }
    }

    @Override
    public void add(DataReceivedEvent event) {
        MessageCommitter committer = event.getCommitter();
        DisjointOffsetRangeSet range = rangesBySession.computeIfAbsent(committer, s -> new DisjointOffsetRangeSet());
        try {
            range.add(event.getRangeToCommit());
        } catch (RuntimeException exception) {
            throw wrapExceptionWithSession(event.getPartitionSession(), exception);
        }
    }

    @Override
    public void commit() {
        rangesBySession.forEach((committer, ranges) -> {
            committer.commitRanges(ranges.getRangesAndClear());

            if (isStopped(committer)) {
                // Ranges of a stopped partition session can't be committed anymore, so it is safe to lose a range
                // added concurrently. Removing the committer allows the stopped read session to be garbage collected
                rangesBySession.remove(committer, ranges);
            }
        });
    }

    @VisibleForTesting
    int getCommittersCount() {
        return rangesBySession.size();
    }

    private static boolean isStopped(MessageCommitter committer) {
        return committer instanceof ReadPartitionCommitter && ((ReadPartitionCommitter) committer).isStopped();
    }
}
