package com.github.basking2.otternet.http;

import com.github.basking2.jiraffet.Jiraffet;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.LogDao;
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
    private Jiraffet jiraffet;

    public ControlService(final Jiraffet jiraffet, final OtterIO io, final OtterLog log) {
        this.jiraffet = jiraffet;
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
    public JoinResponse getJoin(@PathParam("node") final String node) throws IOException, JiraffetIOException {
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
                    target(io.getNodeId()).
                    path("/jiraffet/join");

            final JoinResponse r = wt.request(MediaType.APPLICATION_JSON).buildPost(
                    Entity.entity(new JoinRequest(node), MediaType.APPLICATION_JSON)).invoke(JoinResponse.class);

            jiraffet.setNewLeader(r.getLeader(), r.getTerm());

            return r;
        }
        catch (final Throwable t) {
            LOG.error(t.getMessage(), t);
            throw t;
        }
    }

    @GET
    @Path("info")
    @Produces(MediaType.TEXT_PLAIN)
    public String getInfo() throws JiraffetIOException {
        final LogDao.EntryMeta entryMeta = log.last();
        final StringBuilder sb = new StringBuilder()
                .append("ID: ").append(io.getNodeId())
                .append("\nLeader: ").append(jiraffet.getCurrentLeader())
                .append("\nTerm: ").append(log.getCurrentTerm())
                .append("\nVoted For: ").append(log.getVotedFor())
                .append("\nLast Entry term/index: ").append(entryMeta.getTerm()).append("/").append(entryMeta.getIndex())
                ;

        for (final String s : io.nodes()) {
            sb.append("\n\tNode: ").append(s);
        }

       return sb.append("\n").toString();

    }
}
