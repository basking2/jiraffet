package com.github.basking2.otternet.http;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;
import com.github.basking2.otternet.jiraffet.OtterIO;

@Path("jiraffet")
public class JiraffetJson {

    private OtterIO io;
    
    public JiraffetJson(final OtterIO io) {
        this.io = io;
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
    
    private Map<String, Object> response(final String status){
        final HashMap<String, Object> m = new HashMap<>();

        m.put("status", status);
        
        return m;
    }
}
