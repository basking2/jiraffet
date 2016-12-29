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

import com.github.basking2.jiraffet.Jiraffet;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.otternet.jiraffet.ClientResponse;
import com.github.basking2.otternet.jiraffet.OtterIO;
import com.github.basking2.otternet.jiraffet.OtterLog;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of Raft by Jiraffet on the local node.
 *
 * Calls in here are going to the Jiraffet and related services running.
 *
 * If a remote node tries to join the cluster, it will call /jiraffet/join to do so.
 * If you want to instruct a node to join with a remote system
 */
@Path("jiraffet")
public class JiraffetJsonService {
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetJsonService.class);

    private OtterIO io;
    private OtterLog log;
    private Jiraffet jiraffet;

    public JiraffetJsonService(final Jiraffet jiraffet, final OtterIO io, final OtterLog log) {
        this.jiraffet = jiraffet;
        this.io = io;
        this.log = log;
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
    @Path("vote/response")
    @Produces(MediaType.APPLICATION_JSON)
    public Object postVoteReponse(final RequestVoteResponse msg) {
        io.add(msg);
        return response("OK");
    }
    
    @POST
    @Path("vote/request")
    @Produces(MediaType.APPLICATION_JSON)
    public Object postVoteRequest(final RequestVoteRequest msg) {
        io.add(msg);
        return response("OK");
    }
    
    @POST
    @Path("append/request")
    @Produces(MediaType.APPLICATION_JSON)
    public Object postAppendRequest(final AppendEntriesRequest msg) {
        io.add(msg);
        return response("OK");
    }

    @POST
    @Path("append/response")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Object postAppendResponse(final AppendEntriesResponse msg) {
        io.add(msg);
        return response("OK");
    }

    @POST
    @Path("join")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postJoin(final JoinRequest join) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {

        final Future<ClientResponse> clientResponseFuture = io.clientRequestJoin(join.getId());

        return postResponse(clientResponseFuture);
    }

    @POST
    @Path("leave")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postLeave(final JoinRequest join) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {

        final Future<ClientResponse> clientResponseFuture = io.clientRequestLeave(join.getId());

        return postResponse(clientResponseFuture);
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
            throws IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {

        final byte[] data = IOUtils.toByteArray(postBody);

        final Future<ClientResponse> clientResponseFuture =  io.clientAppendBlob(key, type, data);

        // FIXME - the response model is incorrect.
        return postResponse(clientResponseFuture);

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

    /**
     * Handle a post that involves waiting for a client response.
     *
     * @param clientResponseFuture The client response to wait for.
     * @return The response tranlated into a {@link Response}.
     * @throws URISyntaxException The URI syntax is not correct for the leader.
     * @throws InterruptedException The waiting thread is interrupted.
     */
    private Response postResponse(final Future<ClientResponse> clientResponseFuture) {
        try {
            final JoinResponse joinResponse = new JoinResponse();

            final ClientResponse clientResponse = clientResponseFuture.get(30, TimeUnit.SECONDS);

            // Tell the client who the current leader is.
            joinResponse.setLeader(jiraffet.getCurrentLeader());
            joinResponse.setTerm(log.getCurrentTerm());

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
                return Response.temporaryRedirect(new URI(clientResponse.getLeader())).build();
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
