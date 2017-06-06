package com.github.basking2.otternet.http;

import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.JiraffetLog;
import com.github.basking2.jiraffet.db.LogMyBatis;
import com.github.basking2.otternet.jiraffet.OtterAccess;
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
    final private OtterAccess access;

    public ControlService(final OtterAccess access) {
        this.access = access;
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
     * @param instance The raft instance to join.
     * @return The response from the target node.
     * @throws IOException On an IO exception.
     * @throws JiraffetIOException On a JiraffetRaft exception.
     */
    @GET
    @Path("join/{node}/{instance}")
    @Produces(MediaType.APPLICATION_JSON)
    public JoinResponse getJoin(@PathParam("node") final String node, @PathParam("instance") final String instance) throws IOException, JiraffetIOException {
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
                    path("/"+instance+"/join");

            final JoinResponse r = wt.
                    request(MediaType.APPLICATION_JSON).
                    buildPost(Entity.entity(new JoinRequest(access.getInstance(instance).getNodeId()), MediaType.APPLICATION_JSON)).
                    invoke(JoinResponse.class);

            LOG.info("Got response from node {}. Setting leader {} with term {}.", node, r.getLeader(), r.getTerm());
            access.getRaft(instance).setNewLeader(r.getLeader(), r.getTerm());

            final LogMyBatis log = access.getLog(instance);

            log.deleteBefore(r.getLogCompactionIndex());

            return r;
        }
        catch (final Throwable t) {
            LOG.error(t.getMessage(), t);
            throw t;
        }
    }

    @GET
    @Path("info/{instance}")
    @Produces(MediaType.TEXT_PLAIN)
    public String getInfo(@PathParam("instance") final String instance) throws JiraffetIOException {
        final JiraffetLog.EntryMeta entryMeta = access.getLog(instance).last();
        final StringBuilder sb = new StringBuilder()
                .append("ID: ").append(access.getRaft(instance).getNodeId())
                .append("\nLeader: ").append(access.getRaft(instance).getCurrentLeader())
                .append("\nTerm: ").append(access.getLog(instance).getCurrentTerm())
                .append("\nVoted For: ").append(access.getLog(instance).getVotedFor())
                .append("\nLast Entry term/index: ").append(entryMeta.getTerm()).append("/").append(entryMeta.getIndex())
                ;

        //for (final String s : io.nodes()) {
        //    sb.append("\n\tNode: ").append(s);
        //}

       return sb.append("\n").toString();

    }
}
