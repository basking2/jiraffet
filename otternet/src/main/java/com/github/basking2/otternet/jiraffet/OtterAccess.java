package com.github.basking2.otternet.jiraffet;

import com.github.basking2.jiraffet.*;
import com.github.basking2.jiraffet.messages.ClientRequest;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;

import static java.util.Arrays.asList;

/**
 * Access to many Jiraffet instances.
 */
public class OtterAccess {

    private final ScheduledExecutorService scheduledExecutorService;
    private final JiraffetRaftFactory jiraffetRaftFactory;

    private Map<String, Jiraffet> instances;

    public OtterAccess(
            final JiraffetRaftFactory jiraffetRaftFactory,
            final ScheduledExecutorService scheduledExecutorService
    ) {
        this.instances = new HashMap<>();
        this.jiraffetRaftFactory = jiraffetRaftFactory;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    /**
     * Call {@link JiraffetRaftFactory#getInstance(String)} and make a {@link Jiraffet} instance that uses them.
     *
     * @param instanceName The name of the instances. This is the identifier used to access the instance.
     *
     * @return The concrete instance.
     */
    public Jiraffet getInstance(final String instanceName) throws JiraffetIOException {

        if (instances.containsKey(instanceName)) {
            return instances.get(instanceName);
        }

        final Jiraffet j = new Jiraffet(
                jiraffetRaftFactory.getInstance(instanceName),
                this.scheduledExecutorService
        );

        j.start();

        instances.put(instanceName, j);

        return j;
    }

    public Future<OtterAccessClientResponse> clientRequestJoin(final String instanceName, final String id) throws JiraffetIOException {
        byte[] idBytes = id.getBytes();
        byte[] joinRequest = new byte[1 + idBytes.length];

        ByteBuffer.
                wrap(joinRequest).
                put((byte) OtterLog.LogEntryType.JOIN_ENTRY.ordinal()).
                put(idBytes, 0, idBytes.length);

        return clientRequest(instanceName, joinRequest);
    }

    public Future<OtterAccessClientResponse> clientRequestLeave(final String instanceName, final String id) throws JiraffetIOException {
        byte[] idBytes = id.getBytes();
        byte[] leaveRequest = new byte[1 + idBytes.length];
        leaveRequest[0] = (byte) OtterLog.LogEntryType.LEAVE_ENTRY.ordinal();

        ByteBuffer.
                wrap(leaveRequest).
                put((byte) OtterLog.LogEntryType.LEAVE_ENTRY.ordinal()).
                put(idBytes, 1, idBytes.length);

        return clientRequest(instanceName, leaveRequest);
    }

    public Future<OtterAccessClientResponse> clientAppendBlob(final String instanceName, final String key, final String type, final byte[] data) throws JiraffetIOException {

        byte[] blobBytes = new byte[1 + 4 + key.getBytes().length + 4 + type.getBytes().length + 4 + data.length];
        ByteBuffer blobByteBuffer = ByteBuffer.wrap(blobBytes);

        blobByteBuffer.put((byte)OtterLog.LogEntryType.BLOB_ENTRY.ordinal());

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

    public interface JiraffetRaftFactory {
        JiraffetRaft getInstance(final String instanceName);
    }
}
