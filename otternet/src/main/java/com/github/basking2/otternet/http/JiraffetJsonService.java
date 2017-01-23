package com.github.basking2.otternet.http;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.*;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.github.basking2.jiraffet.JiraffetRaft;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.otternet.jiraffet.OtterAccessClientResponse;
import com.github.basking2.otternet.jiraffet.OtterAccess;
import com.github.basking2.otternet.jiraffet.OtterIO;
import com.github.basking2.otternet.jiraffet.OtterLog;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Raft by JiraffetRaft on the local node.
 *
 * Calls in here are going to the JiraffetRaft and related services running.
 *
 * If a remote node tries to join the cluster, it will call /jiraffet/join to do so.
 * If you want to instruct a node to join with a remote system
 */
@Path("jiraffet")
public class JiraffetJsonService {
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetJsonService.class);

    private OtterIO io;
    private OtterLog log;
    private JiraffetRaft raft;
    private OtterAccess access;

    public JiraffetJsonService(final OtterAccess access, final JiraffetRaft raft, final OtterIO io, final OtterLog log) {
        this.raft = raft;
        this.io = io;
        this.log = log;
        this.access = access;
    }

    @GET
    @Path("ping")
    @Produces(MediaType.APPLICATION_JSON)
    public Object ping() {
        final HashMap<String, String> m = new HashMap<>();

        m.put("status", "OK");
        
        return m;
    }
    

    @POST
    @Path("vote/request")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public RequestVoteResponse postVoteRequest(final RequestVoteRequest msg) throws JiraffetIOException {
        return access.requestVotes(msg);
    }
    
    @POST
    @Path("append/request")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public AppendEntriesResponse postAppendRequest(final AppendEntriesRequest msg) throws JiraffetIOException {
        return access.appendEntries(msg);
    }

    @POST
    @Path("join")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postJoin(final JoinRequest join) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {

        final Future<OtterAccessClientResponse> clientResponseFuture = access.clientRequestJoin(join.getId());

        return postJoinResponse(clientResponseFuture);
    }

    @POST
    @Path("leave")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postLeave(final JoinRequest join) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {

        final Future<OtterAccessClientResponse> clientResponseFuture = access.clientRequestLeave(join.getId());

        return postJoinResponse(clientResponseFuture);
    }

    @POST
    @Path("blob/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.WILDCARD)
    public Response postBlob(
            @PathParam("key") final String key,
            @HeaderParam(HttpHeaders.CONTENT_TYPE) @DefaultValue(MediaType.APPLICATION_OCTET_STREAM) final String type,
            final InputStream postBody
    )
            throws IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException
    {

        final byte[] data = IOUtils.toByteArray(postBody);

        final Future<OtterAccessClientResponse> clientResponseFuture =  access.clientAppendBlob(key, type, data);

        return postBlobResponse(clientResponseFuture, key);
    }

    @GET
    @Path("blob/{key}")
    @Consumes(MediaType.WILDCARD)
    public Response getBlob(@PathParam("key") final String key) throws IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException {
        final OtterLog.Blob blobData = log.getBlob(key);

        if (blobData == null) {
            return Response.
                    status(Response.Status.NOT_FOUND).
                    type(MediaType.APPLICATION_JSON).
                    entity(response("ERROR")).build();
        }

        if (blobData.getData() == null || blobData.getData().length == 0) {
            return Response.noContent().build();
        }

        return Response.ok(blobData.getData()).type(blobData.getType()).build();
    }

    private Response postBlobResponse(final Future<OtterAccessClientResponse> clientResponseFuture, final String key) {
        try {

            final OtterAccessClientResponse clientResponse = clientResponseFuture.get(30, TimeUnit.SECONDS);

            if (clientResponse.isSuccess()) {
                return Response.ok(new JsonResponse()).build();
            }
            else if (io.getNodeId().equals(clientResponse.getLeader())) {
                final JsonResponse jsonResponse = new JsonResponse();
                jsonResponse.setStatus(JsonResponse.ERROR);
                jsonResponse.setMessage(clientResponse.getMessage());
                return Response.serverError().entity(jsonResponse).build();
            }
            else if (clientResponse.getLeader() == null || clientResponse.getLeader().isEmpty()) {
                return Response.
                        status(Response.Status.SERVICE_UNAVAILABLE).
                        type(MediaType.TEXT_PLAIN).
                        entity("A leader is not yet elected.").
                        build();
            }
            else {
                return Response.temporaryRedirect(new URI(clientResponse.getLeader()+"/jiraffet/blob/"+key)).build();
            }

        }
        catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.serverError().entity(new JsonResponse(JsonResponse.ERROR, e.getMessage())).build();

        }
    }

    /**
     * Handle a post that involves waiting for a client response.
     *
     * @param clientResponseFuture The client response to wait for.
     * @return The response tranlated into a {@link Response}.
     * @throws URISyntaxException The URI syntax is not correct for the leader.
     * @throws InterruptedException The waiting thread is interrupted.
     */
    private Response postJoinResponse(final Future<OtterAccessClientResponse> clientResponseFuture) {
        try {
            final JoinResponse joinResponse = new JoinResponse();

            final OtterAccessClientResponse clientResponse = clientResponseFuture.get(30, TimeUnit.SECONDS);

            // Tell the client who the current leader is.
            joinResponse.setLeader(raft.getCurrentLeader());
            joinResponse.setTerm(log.getCurrentTerm());
            joinResponse.setLogId(log.getLogId());
            joinResponse.setLogCompactionIndex(log.first().getIndex());

            if (clientResponse.isSuccess()) {
                joinResponse.setStatus(JsonResponse.OK);
                return Response.ok(joinResponse).build();
            } else if (io.getNodeId().equals(clientResponse.getLeader())) {
                return Response.serverError().entity(joinResponse).build();
            } else if (clientResponse.getLeader() == null) {
                return Response.
                        status(Response.Status.SERVICE_UNAVAILABLE).
                        type(MediaType.TEXT_PLAIN).
                        entity("A leader is not yet elected.").
                        build();
            } else {
                return Response.temporaryRedirect(new URI(clientResponse.getLeader() + "/jiraffet/join")).build();
            }

        }
        catch (final Exception e) {
            LOG.error(e.getMessage(), e);
            return Response.serverError().entity(new JsonResponse(JsonResponse.ERROR, e.getMessage())).build();

        }
    }

    /**
     * Build a simple response object with the given status.
     *
     * @param status Status response to use.
     * @return The response.
     */
    private Map<String, Object> response(final String status){
        final HashMap<String, Object> m = new HashMap<>();

        m.put("status", status);
        
        return m;
    }
}
