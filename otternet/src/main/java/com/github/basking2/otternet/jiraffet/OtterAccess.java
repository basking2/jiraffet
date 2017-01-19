package com.github.basking2.otternet.jiraffet;

import com.github.basking2.jiraffet.*;
import com.github.basking2.jiraffet.messages.ClientRequest;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Arrays.asList;

/**
 * Adds some methods to write user types.
 */
public class OtterAccess extends JiraffetAccess{
    public OtterAccess(Jiraffet jiraffet, JiraffetIO io, LogDao log) {
        super(jiraffet, io, log);
    }

    public OtterAccess(Jiraffet jiraffet, JiraffetIO io, LogDao log, ScheduledExecutorService scheduledExecutorService) {
        super(jiraffet, io, log, scheduledExecutorService);
    }

    public Future<OtterAccessClientResponse> clientRequestJoin(final String id) throws JiraffetIOException {
        byte[] idBytes = id.getBytes();
        byte[] joinRequest = new byte[1 + idBytes.length];

        ByteBuffer.
                wrap(joinRequest).
                put((byte) OtterLog.LogEntryType.JOIN_ENTRY.ordinal()).
                put(idBytes, 0, idBytes.length);

        return clientRequest(joinRequest);
    }

    public Future<OtterAccessClientResponse> clientRequestLeave(final String id) throws JiraffetIOException {
        byte[] idBytes = id.getBytes();
        byte[] leaveRequest = new byte[1 + idBytes.length];
        leaveRequest[0] = (byte) OtterLog.LogEntryType.LEAVE_ENTRY.ordinal();

        ByteBuffer.
                wrap(leaveRequest).
                put((byte) OtterLog.LogEntryType.LEAVE_ENTRY.ordinal()).
                put(idBytes, 1, idBytes.length);

        return clientRequest(leaveRequest);
    }

    public Future<OtterAccessClientResponse> clientAppendBlob(final String key, final String type, final byte[] data) throws JiraffetIOException {

        byte[] blobBytes = new byte[1 + 4 + key.getBytes().length + 4 + type.getBytes().length + 4 + data.length];
        ByteBuffer blobByteBuffer = ByteBuffer.wrap(blobBytes);

        blobByteBuffer.put((byte)OtterLog.LogEntryType.BLOB_ENTRY.ordinal());

        blobByteBuffer.putInt(key.getBytes().length);
        blobByteBuffer.put(key.getBytes());

        blobByteBuffer.putInt(type.getBytes().length);
        blobByteBuffer.put(type.getBytes());

        blobByteBuffer.putInt(data.length);
        blobByteBuffer.put(data);

        return clientRequest(blobBytes);
    }

    public Future<OtterAccessClientResponse> clientRequest(final byte[] message) throws JiraffetIOException {
        final CompletableFuture<OtterAccessClientResponse> future = new CompletableFuture<>();

        append(asList(new ClientRequest() {
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
}
