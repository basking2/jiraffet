package com.github.basking2.otternet.jiraffet;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.basking2.jiraffet.JiraffetIO;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.ClientRequest;
import com.github.basking2.jiraffet.messages.Message;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;

public class OtterIO implements JiraffetIO {
    
    private final ObjectMapper objectMapper;
    private final List<String> nodes;
    private final String nodeId;
    private final BlockingQueue<Message> queue;
    private static final Logger LOG = LoggerFactory.getLogger(OtterIO.class);
    
    public OtterIO(
            final String nodeId,
            final List<String> nodes
    ) {
        this.nodeId = nodeId;
        this.nodes = nodes;
        this.queue = new LinkedBlockingQueue<>();
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public int nodeCount() {
        return nodes.size();
    }

    @Override
    public void requestVotes(RequestVoteRequest req) throws JiraffetIOException {
        for (final String node : nodes) {
            postTo(node + "/jiraffet/vote/request", req);
        }
    }

    @Override
    public void requestVotes(String candidateId, RequestVoteResponse req) throws JiraffetIOException {
        postTo(candidateId + "/jiraffet/vote/response", req);
    }

    @Override
    public void appendEntries(String id, AppendEntriesRequest req) throws JiraffetIOException {
        for (final String node : nodes) {
            postTo(node + "/jiraffet/append/request", req);
        }
    }

    @Override
    public void appendEntries(String id, AppendEntriesResponse resp) throws JiraffetIOException {
        postTo(id + "/jiraffet/append/response", resp);
    }

    @Override
    public List<Message> getMessages(long timeout, TimeUnit timeunit)
        throws JiraffetIOException, TimeoutException, InterruptedException
    {
        final ArrayList<Message> messages = new ArrayList<>();

        final Message m = queue.poll(timeout, timeunit);
        if (m == null) {
            throw new TimeoutException("No messages received.");
        }

        messages.add(m);

        queue.drainTo(messages);
        
        return messages;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public List<String> nodes() {
        return nodes;
    }

    @Override
    public void clientRequest(List<ClientRequest> clientRequests) {
        queue.addAll(clientRequests);
    }

    public Future<ClientResponse> clientRequest(final byte[] message) {
        final CompletableFuture<ClientResponse> future = new CompletableFuture<>();

        queue.add(new ClientRequest() {
            @Override
            public byte[] getData() {
                return message;
            }

            @Override
            public void complete(boolean success, String leader, String msg) {
                future.complete(new ClientResponse(success, leader, msg));
            }
        });

        return future;
    }

    public Future<ClientResponse> clientRequestJoin(final String id) {
        byte[] idBytes = id.getBytes();
        byte[] joinRequest = new byte[1 + idBytes.length];

        ByteBuffer.
                wrap(joinRequest).
                put((byte) OtterLog.LogEntryType.JOIN_ENTRY.ordinal()).
                put(idBytes, 0, idBytes.length);

        return clientRequest(joinRequest);
    }

    public Future<ClientResponse> clientRequestLeave(final String id) {
        byte[] idBytes = id.getBytes();
        byte[] leaveRequest = new byte[1 + idBytes.length];
        leaveRequest[0] = (byte) OtterLog.LogEntryType.LEAVE_ENTRY.ordinal();

        ByteBuffer.
                wrap(leaveRequest).
                put((byte) OtterLog.LogEntryType.LEAVE_ENTRY.ordinal()).
                put(idBytes, 1, idBytes.length);

        return clientRequest(leaveRequest);
    }

    public Future<ClientResponse> clientAppendBlob(final String key, final String type, final byte[] data) {

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

    /**
     * Used to post to other {@link OtterIO} instances running on remote servers.
     * @param url
     * @param message
     */
    private void postTo(final String url, final Message message) {
        try {
            final HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
            con.setInstanceFollowRedirects(true);
            con.setConnectTimeout(500);
            con.setReadTimeout(500);
            con.setDoOutput(true);
            con.setRequestMethod("POST");
            objectMapper.writeValue(con.getOutputStream(), message);
            con.getOutputStream().close();
        }
        catch (final JsonMappingException e) {
            LOG.error("Generating JSON", e);
        }
        catch (final JsonGenerationException e) {
            LOG.error("Generating JSON", e);
        }
        catch (final IOException e) {
            LOG.error("Writing result.", e);
        }
    }
    
    public void add(final Message msg) {
        queue.add(msg);
    }
}
