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

import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.otternet.jiraffet.KeyValueStore;
import com.github.basking2.otternet.jiraffet.OtterAccessClientResponse;
import com.github.basking2.otternet.jiraffet.OtterAccess;
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

    private OtterAccess access;

    public JiraffetJsonService(final OtterAccess access) {
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
    @Path("{instance}/vote/request")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public RequestVoteResponse postVoteRequest(
            @PathParam("instance") final String instance,
            final RequestVoteRequest msg
    ) throws JiraffetIOException {
        return access.getInstance(instance).requestVotes(msg);
    }
    
    @POST
    @Path("{instance}/append/request")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public AppendEntriesResponse postAppendRequest(
            @PathParam("instance") final String instance,
            final AppendEntriesRequest msg
    ) throws JiraffetIOException {
        return access.getInstance(instance).appendEntries(msg);
    }

    @POST
    @Path("{instance}/join")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postJoin(
            @PathParam("instance") final String instance,
            final JoinRequest join
    ) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {

        final Future<OtterAccessClientResponse> clientResponseFuture = access.clientRequestJoin(instance, join.getId());

        return postJoinResponse(instance, clientResponseFuture);
    }

    @POST
    @Path("{instance}/leave")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postLeave(
            @PathParam("instance") final String instance,
            final JoinRequest join
    ) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {

        final Future<OtterAccessClientResponse> clientResponseFuture = access.clientRequestLeave(instance, join.getId());

        return postJoinResponse(instance, clientResponseFuture);
    }

    @POST
    @Path("{instance}/blob/{key}")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.WILDCARD)
    public Response postBlob(
            @PathParam("instance") final String instance,
            @PathParam("key") final String key,
            @HeaderParam(HttpHeaders.CONTENT_TYPE) @DefaultValue(MediaType.APPLICATION_OCTET_STREAM) final String type,
            final InputStream postBody
    )
            throws IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException
    {

        final byte[] data = IOUtils.toByteArray(postBody);

        final Future<OtterAccessClientResponse> clientResponseFuture =  access.clientAppendBlob(instance, key, type, data);

        return postBlobResponse(instance, clientResponseFuture, key);
    }

    @GET
    @Path("{instance}/blob/{key}")
    @Consumes(MediaType.WILDCARD)
    public Response getBlob(
            @PathParam("instance") final String instance,
            @PathParam("key") final String key
    ) throws IOException, InterruptedException, ExecutionException, TimeoutException, URISyntaxException, JiraffetIOException {
        final KeyValueStore.Blob blobData = access.getKeyValueStore(instance).get(key);

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

    private Response postBlobResponse(final String instance, final Future<OtterAccessClientResponse> clientResponseFuture, final String key) {
        try {

            final OtterAccessClientResponse clientResponse = clientResponseFuture.get(30, TimeUnit.SECONDS);

            if (clientResponse.isSuccess()) {
                return Response.ok(new JsonResponse()).build();
            }
            else if (access.getInstance(instance).getNodeId().equals(clientResponse.getLeader())) {
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
                return Response.temporaryRedirect(new URI(clientResponse.getLeader()+"/"+instance+"/blob/"+key)).build();
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
    private Response postJoinResponse(final String instance, final Future<OtterAccessClientResponse> clientResponseFuture) {
        try {
            final JoinResponse joinResponse = new JoinResponse();

            final OtterAccessClientResponse clientResponse = clientResponseFuture.get(30, TimeUnit.SECONDS);

            // Tell the client who the current leader is.
            joinResponse.setLeader(access.getInstance(instance).getCurrentLeader());
            joinResponse.setTerm(access.getLog(instance).getCurrentTerm());
            joinResponse.setLogId("FIXME - need ID");
            joinResponse.setLogCompactionIndex(access.getLog(instance).first().getIndex());

            if (clientResponse.isSuccess()) {
                joinResponse.setStatus(JsonResponse.OK);
                return Response.ok(joinResponse).build();
            } else if (access.getInstance(instance).getNodeId().equals(clientResponse.getLeader())) {
                return Response.serverError().entity(joinResponse).build();
            } else if (clientResponse.getLeader() == null) {
                return Response.
                        status(Response.Status.SERVICE_UNAVAILABLE).
                        type(MediaType.TEXT_PLAIN).
                        entity("A leader is not yet elected.").
                        build();
            } else {
                return Response.temporaryRedirect(new URI(clientResponse.getLeader() + "/"+instance+"/join")).build();
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
