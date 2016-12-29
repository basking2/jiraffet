package com.github.basking2.otternet.jiraffet;

import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.LogDao;
import com.github.basking2.otternet.OtterNet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    private int offset = 1;
    private List<byte[]> dataLog = new ArrayList<>();
    private List<EntryMeta> metaLog = new ArrayList<>();

    /**
     * When blobs are applied, they are linked here for fast access.
     */
    private Map<String, Blob> blobStorage;

    private final OtterNet otterNet;
    private final OtterIO io;

    public OtterLog(final OtterNet otterNet, final OtterIO io) {
        this.otterNet = otterNet;
        this.io = io;
        this.blobStorage = new HashMap<>();

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

        else if (offsetIndex < dataLog.size()) {
            dataLog.set(offsetIndex, data);
            metaLog.set(offsetIndex, new EntryMeta(term, offsetIndex));
        }

        else {
            throw new IllegalArgumentException("Cannot set arbitrary log entries in the future.");
        }

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

        LOG.info("Applying log {}.", index);

        final byte[] data;

        try {
            data = read(index);
        } catch (JiraffetIOException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

        LOG.info("Log entry {} is {} bytes long.", index, data.length);

        switch (LogEntryType.fromByte(data[0])) {
            case NOP_ENTRY:
                // Nop!
                break;
            case BLOB_ENTRY:
                final ByteBuffer blobByteBuffer = ByteBuffer.wrap(data);

                // Skip type byte.
                blobByteBuffer.position(1);

                final int blobKeyLen = blobByteBuffer.getInt();
                final byte[] blobKeyBytes = new byte[blobKeyLen];
                blobByteBuffer.get(blobKeyBytes);

                final int blobTypeLen = blobByteBuffer.getInt();
                final byte[] blobTypeBytes = new byte[blobTypeLen];
                blobByteBuffer.get(blobTypeBytes);

                final int blobDataLen = blobByteBuffer.getInt();
                final byte[] blobDataBytes = new byte[blobDataLen];
                blobByteBuffer.get(blobDataBytes);

                blobStorage.put(new String(blobKeyBytes), new Blob(new String(blobTypeBytes), blobDataBytes));
                break;
            case JOIN_ENTRY:
                final String joinHost = new String(data, 1, data.length - 1);
                if (joinHost.equalsIgnoreCase(io.getNodeId())) {
                    throw new IllegalStateException("Cannot join ourselves: "+joinHost);
                }
                // Remove then add to ensure no duplicates.
                io.nodes().remove(joinHost);
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
        final List<byte[]> dataLogView = dataLog.subList(offsetIndex, dataLog.size());
        final List<EntryMeta> metaLogView = metaLog.subList(offsetIndex, metaLog.size());

        // Now create independent logs from those views, freeing the memory for the previous entries.
        dataLog = new ArrayList<>(dataLogView);
        metaLog = new ArrayList<>(metaLogView);

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
         * A named sequence of bytes.
         *
         * This is encoded with the 1-byte type, then the key length, then the key in bytes.
         * Then the type length, and the type in bytes.
         * Then the data length, and the data in bytes.
         */
        BLOB_ENTRY;

        public static LogEntryType fromByte(byte b) {
            if (b < 0 || b >= values().length) {
                return NOP_ENTRY;
            }

            return values()[b];
        }
    }

    public Blob getBlob(final String key) {
        return blobStorage.get(key);
    }

    public static class Blob {
        private byte[] data;
        private String type;

        public Blob(final String type, final byte[] data) {
            this.data = data;
            this.type = type;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
