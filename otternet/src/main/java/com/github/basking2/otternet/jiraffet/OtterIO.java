package com.github.basking2.otternet.jiraffet;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
            throws JiraffetIOException, TimeoutException, InterruptedException {
        ArrayList<Message> messages = new ArrayList<>();
        
        messages.add(queue.poll(timeout, timeunit));
        
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

    private void postTo(final String url, final Message message) {
        try {
        final HttpURLConnection con = (HttpURLConnection)new URL(url).openConnection();
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
