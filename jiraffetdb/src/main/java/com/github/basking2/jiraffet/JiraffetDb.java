package com.github.basking2.jiraffet;

import java.io.IOException;
import java.net.SocketAddress;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.basking2.jiraffet.db.LogDaoMyBatis;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.util.AppConfiguration;

/**
 * How to configure the TCP app.
 */
public class JiraffetDb {
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetDb.class);

    private AppConfiguration appConfiguration;
    private Jiraffet jiraffet;
    private LogDao log;
    private JiraffetIO io;
    private String id;

    public JiraffetDb() {
        this.appConfiguration = new AppConfiguration(this.getClass());
    }

    private JiraffetIO buildIo() throws IOException {
        final String thisNode = appConfiguration.getString("jiraffetdb.this.node").trim();
        final String[] nodesProp = appConfiguration.getString("jiraffetdb.nodes").split(",");

        // Trim all node names.
        for (int i = 0; i < nodesProp.length; ++i) {
            nodesProp[i] = nodesProp[i].trim();
        }

        if ("auto".equals(thisNode)) {

            IOException lastException = null;

            for (final String node : nodesProp) {
                try {
                    return buildIo(node, nodesProp);
                }
                catch (final IOException e) {
                    lastException = e;
                }
            }

            throw lastException;
        } else {
            return buildIo(thisNode, nodesProp);
        }
    }

    /**
     * Try to builde a {@link JiraffetTcpIO} binding {@code thisNode} as the listening ip.
     *
     * The array of {@code nodes} is copied excluding any values that match {@code thisNode}'s value.
     *
     * @param thisNode This nodes proposed IP and port.
     * @param nodes The list of all known nodes, maybe including {@code thisNode}.
     * @return An IO object.
     * @throws IOException On bind errors.
     */
    private JiraffetIO buildIo(final String thisNode, final String[] nodes) throws IOException {
        final List<String> otherNodes = new ArrayList<String>(nodes.length);

        for (final String node : nodes) {
            if (!node.equals(thisNode)) {
                otherNodes.add(node);
            }
        }

        id = thisNode;

        return new JiraffetTcpIO(thisNode, otherNodes);
    }


    public void start() throws SQLException, ClassNotFoundException, IOException {
        LOG.debug("Starting.");

        com.github.basking2.jiraffet.db.JiraffetDb db = new com.github.basking2.jiraffet.db.JiraffetDb(this.appConfiguration.getString("jiraffetdb.db"));

        this.log = db.getLogDao();

        // public JiraffetTcpIO(final SocketAddress listen, final List<String> nodes) throws IOException {
        this.io = buildIo();

        this.jiraffet = new Jiraffet(id, log, io);

        //this.jiraffet.setIo(io);
        //this.jiraffet.setLog(log);

        try {
            jiraffet.run();
        }
        catch (final IOException e) {
            LOG.error("Running Jiraffet.", e);
        }
    }
}
