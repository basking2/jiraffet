package com.github.basking2.otternet.http;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.otternet.jiraffet.ClientResponse;
import com.github.basking2.otternet.jiraffet.OtterIO;
import com.github.basking2.otternet.jiraffet.OtterLog;

@Path("jiraffet")
public class JiraffetJson {

    private OtterIO io;
    private OtterLog log;

    public JiraffetJson(final OtterIO io, final OtterLog log) {
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
    public Response postJoin(final JoinRequest join) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException {

        final ClientResponse clientResponse = io.clientRequest(("join "+join.getId()).getBytes()).get(30, TimeUnit.SECONDS);

        final JoinResponse joinResponse = new JoinResponse();

        joinResponse.setLeader(clientResponse.getLeader());

        if (clientResponse.isSuccess()) {
            io.nodes().add(join.getId());
            joinResponse.setStatus(JsonResponse.OK);
            return Response.ok(joinResponse).build();
        }
        else if (io.getNodeId().equals(clientResponse.getLeader())) {
            return Response.serverError().entity(joinResponse).build();
        }
        else {
            return Response.temporaryRedirect(new URI(clientResponse.getLeader())).build();
        }
    }

    @POST
    @Path("leave")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public Response postLeave(final JoinRequest join) throws InterruptedException, ExecutionException, TimeoutException, URISyntaxException {

        final ClientResponse clientResponse = io.clientRequest(("leave "+join.getId()).getBytes()).get(30, TimeUnit.SECONDS);

        final JoinResponse joinResponse = new JoinResponse();

        joinResponse.setLeader(clientResponse.getLeader());

        if (clientResponse.isSuccess()) {
            io.nodes().add(join.getId());
            joinResponse.setStatus(JsonResponse.OK);
            return Response.ok(joinResponse).build();
        }
        else if (io.getNodeId().equals(clientResponse.getLeader())) {
            return Response.serverError().entity(joinResponse).build();
        }
        else {
            return Response.temporaryRedirect(new URI(clientResponse.getLeader())).build();
        }
    }

    private Map<String, Object> response(final String status){
        final HashMap<String, Object> m = new HashMap<>();

        m.put("status", status);
        
        return m;
    }
}
