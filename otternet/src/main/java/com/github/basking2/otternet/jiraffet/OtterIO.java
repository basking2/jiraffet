package com.github.basking2.otternet.jiraffet;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.JiraffetIO;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.ClientRequest;
import com.github.basking2.jiraffet.messages.Message;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

public class OtterIO implements JiraffetIO {
    
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
    }

    @Override
    public int nodeCount() {
        return nodes.size();
    }

    @Override
    public List<RequestVoteResponse> requestVotes(RequestVoteRequest req) throws JiraffetIOException {
        final List<RequestVoteResponse> responses = new ArrayList<>(nodes.size());

        for (final String node : nodes) {

            final RequestVoteResponse r = ClientBuilder.newBuilder().build().register(JacksonFeature.class).
                    target(node).
                    path("/jiraffet/vote/request").
                    request(MediaType.APPLICATION_JSON).
                    buildPost(Entity.entity(req, MediaType.APPLICATION_JSON)).
                    invoke(RequestVoteResponse.class);

            responses.add(r);
        }

        return responses;
    }

    @Override
    public List<AppendEntriesResponse> appendEntries(List<String> id, List<AppendEntriesRequest> req) throws JiraffetIOException {
        final List<AppendEntriesResponse> responses = new ArrayList<>(nodes.size());

        final Iterator<String> idItr = id.iterator();
        final Iterator<AppendEntriesRequest> reqItr = req.iterator();

        while (idItr.hasNext() && reqItr.hasNext()) {
            final String node = idItr.next();
            final AppendEntriesRequest request = reqItr.next();

            final AppendEntriesResponse r = ClientBuilder.newBuilder().build().register(JacksonFeature.class).
                    target(node).
                    path("/jiraffet/append/request").
                    request(MediaType.APPLICATION_JSON).
                    buildPost(Entity.entity(request, MediaType.APPLICATION_JSON)).
                    invoke(AppendEntriesResponse.class);

            responses.add(r);
        }


        return responses;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public List<String> nodes() {
        return nodes;
    }
}
