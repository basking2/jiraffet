package com.github.basking2.otternet.jiraffet;

import com.github.basking2.jiraffet.*;
import com.github.basking2.jiraffet.db.KeyValueMyBatis;
import com.github.basking2.jiraffet.db.LogDbManager;
import com.github.basking2.jiraffet.db.LogMyBatis;
import com.github.basking2.jiraffet.messages.ClientRequest;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static java.util.Arrays.asList;

/**
 * Access to many Jiraffet instances.
 */
public class OtterAccess implements Closeable {

    private final ScheduledExecutorService scheduledExecutorService;
    private final JiraffetIoFactory ioFactory;
    private final JiraffetLogFactory logFactory;

    private Map<String, InstanceAndLog> instances;

    public OtterAccess(
            final JiraffetIoFactory ioFactory,
            final JiraffetLogFactory logFactory,
            final ScheduledExecutorService scheduledExecutorService
    ) {
        this.instances = new HashMap<>();
        this.ioFactory = ioFactory;
        this.logFactory = logFactory;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    public Jiraffet getInstance(final String instanceName) throws JiraffetIOException {
        return getInstanceAndLog(instanceName).instance;
    }

    public JiraffetRaft getRaft(final String instanceName) throws JiraffetIOException {
        return getInstanceAndLog(instanceName).raft;
    }

    public LogMyBatis getLog(final String instanceName) throws JiraffetIOException {
        return getInstanceAndLog(instanceName).log;
    }

    public KeyValueMyBatis getKeyValueStore(final String instanceName) throws JiraffetIOException {
        return getInstanceAndLog(instanceName).keyValue;
    }

    private InstanceAndLog getInstanceAndLog(final String instanceName) throws JiraffetIOException {

        // If we have the record, return it.
        if (instances.containsKey(instanceName)) {
            return instances.get(instanceName);
        }

        final OtterIO io = ioFactory.getInstance(instanceName);
        final LogDbManager mgr = logFactory.getInstance(instanceName);
        final LogMyBatis log = mgr.getLogDao();
        final KeyValueMyBatis keyValue = mgr.getKeyValue();

        log.addApplier(new OtterLogApplier(keyValue, io, log));

        final JiraffetRaft raft = new JiraffetRaft(log, io);

        final Jiraffet jiraffet = new Jiraffet(raft, scheduledExecutorService);

        final InstanceAndLog ial = new InstanceAndLog(jiraffet, raft, log, keyValue);

        instances.put(instanceName, ial);

        return ial;
    }

    public Future<OtterAccessClientResponse> clientRequestJoin(final String instanceName, final String id) throws JiraffetIOException {
        byte[] idBytes = id.getBytes();
        byte[] joinRequest = new byte[1 + idBytes.length];

        ByteBuffer.
                wrap(joinRequest).
                put((byte) LogEntryType.JOIN_ENTRY.ordinal()).
                put(idBytes, 0, idBytes.length);

        return clientRequest(instanceName, joinRequest);
    }

    public Future<OtterAccessClientResponse> clientRequestLeave(final String instanceName, final String id) throws JiraffetIOException {
        byte[] idBytes = id.getBytes();
        byte[] leaveRequest = new byte[1 + idBytes.length];
        leaveRequest[0] = (byte) LogEntryType.LEAVE_ENTRY.ordinal();

        ByteBuffer.
                wrap(leaveRequest).
                put((byte) LogEntryType.LEAVE_ENTRY.ordinal()).
                put(idBytes, 1, idBytes.length);

        return clientRequest(instanceName, leaveRequest);
    }

    public Future<OtterAccessClientResponse> clientAppendBlob(final String instanceName, final String key, final String type, final byte[] data) throws JiraffetIOException {

        byte[] blobBytes = new byte[1 + 4 + key.getBytes().length + 4 + type.getBytes().length + 4 + data.length];
        ByteBuffer blobByteBuffer = ByteBuffer.wrap(blobBytes);

        blobByteBuffer.put((byte)LogEntryType.BLOB_ENTRY.ordinal());

        blobByteBuffer.putInt(key.getBytes().length);
        blobByteBuffer.put(key.getBytes());

        blobByteBuffer.putInt(type.getBytes().length);
        blobByteBuffer.put(type.getBytes());

        blobByteBuffer.putInt(data.length);
        blobByteBuffer.put(data);

        return clientRequest(instanceName, blobBytes);
    }

    public Future<OtterAccessClientResponse> clientRequest(final String instanceName, final byte[] message) throws JiraffetIOException {
        final Jiraffet jiraffet = getInstance(instanceName);

        final CompletableFuture<OtterAccessClientResponse> future = new CompletableFuture<>();

        jiraffet.append(asList(new ClientRequest() {
            @Override
            public byte[] getData() {
                return message;
            }

            @Override
            public void complete(boolean success, String leader, String msg) {
                future.complete(new OtterAccessClientResponse(success, leader, msg));
            }
        }));

        return future;
    }

    @Override
    public void close() throws IOException {
        instances.forEach((key, instanceAndLog) -> {
            instanceAndLog.instance.stop();
        });
    }

    public interface JiraffetIoFactory {
        OtterIO getInstance(final String instanceName);
    }

    public interface JiraffetLogFactory {
        LogDbManager getInstance(final String instanceName);
    }

    private static class InstanceAndLog {
        public final LogMyBatis log;
        public final Jiraffet instance;
        public final JiraffetRaft raft;
        public final KeyValueMyBatis keyValue;

        public InstanceAndLog(final Jiraffet instance, final JiraffetRaft raft, final LogMyBatis log, final KeyValueMyBatis keyValue) {
            this.instance = instance;
            this.raft = raft;
            this.log = log;
            this.keyValue = keyValue;
        }
    }
}
