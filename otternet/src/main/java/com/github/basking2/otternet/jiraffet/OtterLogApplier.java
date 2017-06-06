package com.github.basking2.otternet.jiraffet;

import com.github.basking2.jiraffet.JiraffetLog;
import com.github.basking2.jiraffet.db.KeyValueMyBatis;
import com.github.basking2.jiraffet.db.LogMyBatis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Logic to apply a log record.
 */
public class OtterLogApplier implements LogMyBatis.Applier {
    private static final Logger LOG = LoggerFactory.getLogger(OtterLogApplier.class);

    final private JiraffetLog log;
    final private OtterIO io;
    final private KeyValueMyBatis keyValue;

    public OtterLogApplier(final KeyValueMyBatis keyValue, final OtterIO io, final JiraffetLog log) {
        this.log = log;
        this.io = io;
        this.keyValue = keyValue;
    }

    @Override
    public void apply(int index) throws Exception {

        final byte[] data;

        data = log.getLogEntry(index).getData();

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

                keyValue.put(new String(blobKeyBytes), new String(blobTypeBytes), blobDataBytes);
                break;
            case JOIN_ENTRY:
                final String joinHost = new String(data, 1, data.length - 1);
                if (joinHost.equalsIgnoreCase(io.getNodeId())) {
                    LOG.info("Cannot join ourselves so this is implicitly applied: {}", joinHost);
                }
                else {
                    // Remove then add to ensure no duplicates.
                    io.nodes().remove(joinHost);
                    io.nodes().add(joinHost);
                }
                break;
            case LEAVE_ENTRY:
                final String leaveHost = new String(data, 1, data.length - 1);
                io.nodes().remove(leaveHost);
                break;
            case SNAPSHOT_ENTRY:
                final int firstIndex = ByteBuffer.wrap(data).getInt(1);
                deleteBefore(firstIndex);
                break;
            default:
                throw new IllegalStateException("Unexpected data type: " + data[0]);
        }
    }

    private void deleteBefore(int firstIndex) {
        // FIXME - write this.

    }


}
