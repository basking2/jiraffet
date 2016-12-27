package com.github.basking2.otternet.jiraffet;

import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.LogDao;
import com.github.basking2.otternet.OtterNet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * A simplistic implementation.
 */
public class OtterLog implements LogDao {
    private static final Logger LOG = LoggerFactory.getLogger(OtterLog.class);
    private int currentTerm;
    private String votedFor;

    /**
     * Offset of the database. This supports log compaction.
     */
    private int offset = 0;
    private List<byte[]> dataLog = new ArrayList<>();
    private List<EntryMeta> metaLog = new ArrayList<>();

    private final OtterNet otterNet;
    private final OtterIO io;

    public OtterLog(final OtterNet otterNet, final OtterIO io) {
        this.otterNet = otterNet;
        this.io = io;

    }

    @Override
    public void setCurrentTerm(int currentTerm) throws JiraffetIOException {
        this.currentTerm = currentTerm;
    }

    @Override
    public int getCurrentTerm() throws JiraffetIOException {
        return currentTerm;
    }

    @Override
    public void setVotedFor(String id) throws JiraffetIOException {
        this.votedFor = id;
    }

    @Override
    public String getVotedFor() throws JiraffetIOException {
        return votedFor;
    }

    @Override
    public EntryMeta getMeta(int index) throws JiraffetIOException {
        final int offsetIndex = index - offset;
        if (offsetIndex >= 0 && offsetIndex < metaLog.size()) {
            return metaLog.get(offsetIndex);
        }

        return new EntryMeta(0, offset);
    }

    @Override
    public byte[] read(int index) throws JiraffetIOException {

        final int offsetIndex = index - offset;

        if (offsetIndex >= 0 && offsetIndex < dataLog.size()) {
            return dataLog.get(offsetIndex);
        }

        return null;
    }

    @Override
    public boolean hasEntry(int index, int term) throws JiraffetIOException {
        final EntryMeta m = getMeta(index);

        return m.getTerm() == term;
    }

    @Override
    public void write(int term, int index, byte[] data) throws JiraffetIOException {

        final int offsetIndex = index - offset;

        if (offsetIndex == dataLog.size()) {
            dataLog.add(data);
            metaLog.add(new EntryMeta(term, offsetIndex));
        }

        if (offsetIndex < dataLog.size()) {
            dataLog.set(offsetIndex, data);
            metaLog.set(offsetIndex, new EntryMeta(term, offsetIndex));
        }

        throw new IllegalArgumentException("Cannot set arbitrary log entries in the future.");

    }

    @Override
    public void remove(int index) throws JiraffetIOException {
        final int offsetIndex = index - offset;

        if (offsetIndex < 0) {
            return;
        }

        for (int removeIndex = dataLog.size()-1; removeIndex >= offsetIndex; --removeIndex) {
            dataLog.remove(removeIndex);
            metaLog.remove(removeIndex);
        }
    }

    @Override
    public void apply(int index) throws IllegalStateException {
        final int offsetIndex = index - offset;

        final byte[] data;

        try {
            data = read(index);
        } catch (JiraffetIOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

        switch (LogEntryType.fromByte(data[0])) {
            case NOP_ENTRY:
                // Nop!
                break;
            case APP_DATA:
                // FIXME - write this.
                break;
            case JOIN_ENTRY:
                final String joinHost = new String(data, 1, data.length - 1);
                io.nodes().add(joinHost);
                break;
            case LEAVE_ENTRY:
                final String leaveHost = new String(data, 1, data.length - 1);
                io.nodes().remove(leaveHost);
                break;
            case SNAPSHOT_ENTRY:
                final int firstIndex = ByteBuffer.wrap(data).getInt(1);
                purgeBefore(firstIndex);
                break;
            default:
                throw new IllegalStateException("Unexpected data type: " + data[0]);
        }
    }

    /**
     * Delete all entries before the given index.
     *
     * This effectively compacts the log.
     *
     * The {@link #offset} field is set to the value of index as the first element in the new arrays is the
     * lowest and first index we have.
     * @param index
     */
    private void purgeBefore(final int index) {
        int offsetIndex = index - offset;

        // Create a view into the logs.
        final List dataLogView = dataLog.subList(offsetIndex, dataLog.size());
        final List metaLogView = metaLog.subList(offsetIndex, metaLog.size());

        // Now create independent logs from those views, freeing the memory for the previous entries.
        dataLog = new ArrayList(dataLogView);
        metaLog = new ArrayList(metaLogView);

        offset = index;
    }

    @Override
    public EntryMeta last() throws JiraffetIOException {
        if (metaLog.isEmpty()) {
            return new EntryMeta(0, offset);
        }

        return metaLog.get(metaLog.size()-1);
    }

    /**
     * The type of log entry stored. This determines how the log is applied.
     */
    enum LogEntryType {
        /**
         * A log entry that has no effect. Useful for setting barrier versions.
         */
        NOP_ENTRY,

        /**
         * A node is added to the cluster. Contents is a string.
         */
        JOIN_ENTRY,
        /**
         * A node is removed from the cluster. Contents is a string.
         */
        LEAVE_ENTRY,

        /**
         * This signals that the log should be compacted from the start up to the encoded integer index.
         */
        SNAPSHOT_ENTRY,

        /**
         * Application data. This is handed off to the app processor to be integrated into the user application.
         */
        APP_DATA;

        public static LogEntryType fromByte(byte b) {
            if (b < 0 || b >= values().length) {
                return NOP_ENTRY;
            }

            return values()[b];
        }
    }

}
