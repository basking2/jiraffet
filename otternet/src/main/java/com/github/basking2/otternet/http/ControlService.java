package com.github.basking2.otternet.http;

import com.github.basking2.jiraffet.JiraffetRaft;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.JiraffetLog;
import com.github.basking2.otternet.jiraffet.OtterIO;
import com.github.basking2.otternet.jiraffet.OtterLog;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
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
    private JiraffetRaft raft;

    public ControlService(final JiraffetRaft raft, final OtterIO io, final OtterLog log) {
        this.raft = raft;
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
     *
     * @param node The node to join to and seek to be our new leader.
     * @return The response from the target node.
     * @throws IOException On an IO exception.
     * @throws JiraffetIOException On a JiraffetRaft exception.
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
                    target(node).
                    path("/jiraffet/join");

            final JoinResponse r = wt.
                    request(MediaType.APPLICATION_JSON).
                    buildPost(Entity.entity(new JoinRequest(io.getNodeId()), MediaType.APPLICATION_JSON)).
                    invoke(JoinResponse.class);

            LOG.info("Got response from node {}. Setting leader {} with term {}.", node, r.getLeader(), r.getTerm());
            raft.setNewLeader(r.getLeader(), r.getTerm());
            log.deleteBefore(r.getLogCompactionIndex());

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
        final JiraffetLog.EntryMeta entryMeta = log.last();
        final StringBuilder sb = new StringBuilder()
                .append("ID: ").append(io.getNodeId())
                .append("\nLeader: ").append(raft.getCurrentLeader())
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
