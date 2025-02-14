package tech.ydb.topic.write;

/**
 * @author Nikolay Perfilov
 */
public class WriteAck {
    private final long seqNo;
    private final State state;
    private final Details details;

    public WriteAck(long seqNo, State state, Details details) {
        this.seqNo = seqNo;
        this.state = state;
        this.details = details;
    }

    public enum State {
        WRITTEN,
        ALREADY_WRITTEN,
        WRITTEN_IN_TX
    }

    public static class Details {
        private final long offset;

        public Details(long offset) {
            this.offset = offset;
        }

        public long getOffset() {
            return offset;
        }
    }

    public long getSeqNo() {
        return seqNo;
    }

    public State getState() {
        return state;
    }

    /**
     * Get details about written offsets
     * @return {@link Details} with written offsets if state is {@link State#WRITTEN} or null otherwise
     */
    public Details getDetails() {
        return details;
    }
}
