package com.github.basking2.otternet.jiraffet;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.*;

import com.github.basking2.otternet.util.Futures;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.JiraffetIO;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

public class OtterIO implements JiraffetIO {

    public static final String DEFAULT_INSTANCE_NAME = "jiraffet";

    /**
     * The name of the raft instance to be communicated with.
     */
    private final String instanceName;
    private final List<String> nodes;
    private final String nodeId;
    private static final Logger LOG = LoggerFactory.getLogger(OtterIO.class);

    private final ExecutorService executorService;

    /**
     * How OtterNet wants to build a client.
     */
    private final ClientConfig clientBuilderConfiguration;

    /**
     * Constructor that creates an independent thread pool for IO operation.
     *
     * Since IO typically blocks its calling thread, it is not expected that these threads will overly burden the
     * CPUs on a system, and so may exist along side other {@link ExecutorService}s that use other thread pools.
     * The waste involved in this approach is that Threads do block out a call stack of memory and
     * take up time in the CPU scheduler.
     *
     * There is another constructor that allows the user to provide their own {@link ExecutorService}.
     *
     * The executor is created by {@link Executors#newCachedThreadPool()}.
     *
     * @param instanceName The name of the raft instance to communicate with.
     * @param nodeId This node's network ID. Typically {@code http://myhost:myport}.
     * @param nodes A list of node IDs we can connect too.
     *
     * @see #OtterIO(String, String, List, ExecutorService)
     */
    public OtterIO(
            final String instanceName,
            final String nodeId,
            final List<String> nodes
    )
    {
        this(instanceName, nodeId, nodes, Executors.newCachedThreadPool());
    }

    /**
     * Constructor that provides the user access to defining the {@link ExecutorService}.
     *
     * @param instanceName The name of the raft instance to communicate with.
     * @param nodeId This node's network ID. Typically {@code http://myhost:myport}.
     * @param nodes A list of node IDs we can connect too.
     * @param executorService The executor service that will be used for IO operation.
     */
    public OtterIO(
            final String instanceName,
            final String nodeId,
            final List<String> nodes,
            final ExecutorService executorService
    ) {
        this.instanceName = instanceName;
        this.nodeId = nodeId;
        this.nodes = nodes;
        this.executorService = executorService;

        this.clientBuilderConfiguration = new ClientConfig().
            property(ClientProperties.READ_TIMEOUT, 1000).
            property(ClientProperties.CONNECT_TIMEOUT, 1000).
            register(JacksonFeature.class);
    }

    @Override
    public int nodeCount() {
        return nodes.size();
    }

    @Override
    public List<RequestVoteResponse> requestVotes(final RequestVoteRequest req) throws JiraffetIOException {
        final List<RequestVoteResponse> responses = new ArrayList<>(nodes.size());
        final List<Future<RequestVoteResponse>> responseFutures = new ArrayList<>(nodes.size());

        // Build futures list.
        for (final String node : nodes) {
            responseFutures.add(requestVoteFuture(req, node));
        }

        // Collect futures.
        Futures.getAll(responseFutures, responses, 5, TimeUnit.SECONDS);

        return responses;
    }

    private Future<RequestVoteResponse> requestVoteFuture(final RequestVoteRequest req, final String node) throws JiraffetIOException {
        return executorService.submit(new Callable<RequestVoteResponse>() {
            @Override
            public RequestVoteResponse call() throws Exception {
                return ClientBuilder.newClient(clientBuilderConfiguration).
                        target(node).
                        path("/"+instanceName+"/vote/request").
                        request(MediaType.APPLICATION_JSON).
                        buildPost(Entity.entity(req, MediaType.APPLICATION_JSON)).
                        invoke(RequestVoteResponse.class);
            }
        });
    }

    @Override
    public List<AppendEntriesResponse> appendEntries(List<String> id, List<AppendEntriesRequest> req) throws JiraffetIOException {
        final List<AppendEntriesResponse> responses = new ArrayList<>(nodes.size());
        final List<Future<AppendEntriesResponse>> responsesFutures = new ArrayList<>(nodes.size());

        final Iterator<String> idItr = id.iterator();
        final Iterator<AppendEntriesRequest> reqItr = req.iterator();

        while (idItr.hasNext() && reqItr.hasNext()) {
            final String node = idItr.next();
            final AppendEntriesRequest request = reqItr.next();

            responsesFutures.add(appendEntriesResponseFuture(request, node));
        }

        Futures.getAll(responsesFutures, responses, 5, TimeUnit.SECONDS);

        return responses;
    }

    private Future<AppendEntriesResponse> appendEntriesResponseFuture(final AppendEntriesRequest req, final String node) {
        return executorService.submit(new Callable<AppendEntriesResponse>() {

            @Override
            public AppendEntriesResponse call() throws Exception {
                return ClientBuilder.newClient(clientBuilderConfiguration).
                        target(node).
                        path("/"+instanceName+"/append/request").
                        request(MediaType.APPLICATION_JSON).
                        buildPost(Entity.entity(req, MediaType.APPLICATION_JSON)).
                        invoke(AppendEntriesResponse.class);
            }
        });
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
