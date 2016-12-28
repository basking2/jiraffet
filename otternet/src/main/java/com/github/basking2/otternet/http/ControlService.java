package com.github.basking2.otternet.http;

import com.github.basking2.otternet.jiraffet.OtterIO;
import com.github.basking2.otternet.jiraffet.OtterLog;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.*;
import java.util.HashMap;
import java.util.Map;

/**
 * This exposes a service to control the local node.
 */
@Path("control")
public class ControlService {
    private static final Logger LOG = LoggerFactory.getLogger(ControlService.class);
    private OtterIO io;
    private OtterLog log;

    public ControlService(final OtterIO io, final OtterLog log) {
        this.io = io;
        this.log = log;
    }

    /**
     * Check if the control service is here.
     *
     * @return "pong"
     */
    @GET
    @Path("ping")
    @Produces(MediaType.TEXT_PLAIN)
    public String getPong() {
        return "pong";
    }

    /**
     * Instruct this node to join another cluster.
     */
    @GET
    @Path("join/{node}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getJoin(@PathParam("node") final String node) throws IOException {
        try {
            final Map<String, String> map = new HashMap<>();

            final URL url = new URL(node);

            //final HttpURLConnection httpURLConnection = (HttpURLConnection)url.openConnection();
            //httpURLConnection.setInstanceFollowRedirects(true);
            //httpURLConnection.getOutputStream();
            final WebTarget wt = ClientBuilder.
                    newBuilder().
                    build().
                    register(JacksonFeature.class).
                    target(node).
                    path("/jiraffet/join");

            final Response r = wt.request(MediaType.APPLICATION_JSON).buildPost(
                    Entity.entity(new JoinRequest(node), MediaType.APPLICATION_JSON)).invoke();

            return r;
        }
        catch (final Throwable t) {
            LOG.error(t.getMessage(), t);
            throw t;
        }
    }

}
