package com.github.basking2.otternet.http;

import java.util.HashMap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("jiraffet")
public class JiraffetJson {
    @GET
    @Path("ping")
    @Produces(MediaType.APPLICATION_JSON)
    public Object ping() {
        HashMap<String, String> m = new HashMap<>();
        m.put("status", "OK");
        
        return m;
    }

}
