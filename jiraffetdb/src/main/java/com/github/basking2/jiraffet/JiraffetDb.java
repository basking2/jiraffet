package com.github.basking2.jiraffet;

import java.io.IOException;
import java.sql.SQLException;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.db.LogDaoDbManager;
import com.github.basking2.jiraffet.util.App;

/**
 * How to configure the TCP app.
 */
public class JiraffetDb {
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetDb.class);

    private Configuration appConfiguration;
    private Jiraffet jiraffet;
    private LogDao log;
    private JiraffetTcpIO io;

    public JiraffetDb() throws ConfigurationException {
        this.appConfiguration = new App(this.getClass()).buildConfiguration();
    }

    private JiraffetTcpIO buildIo() throws IOException {
        final String thisNode = appConfiguration.getString("jiraffetdb.this.node").trim();
        final String[] nodesProp = appConfiguration.getString("jiraffetdb.nodes").split(",");

        final JiraffetTcpIO io = JiraffetTcpIOFactory.buildIo(thisNode, nodesProp);

        return io;
    }

    public LogDao buildLogDao() throws SQLException, ClassNotFoundException {
        String dbHome = appConfiguration.getString("jiraffetdb.db");

        if (dbHome == null) {
            dbHome = appConfiguration.getString("jiraffetdb.home") + "/db";
        }

        final LogDaoDbManager db = new LogDaoDbManager(dbHome);

        return db.getLogDao();
    }


    public void start() {
        try {
            this.log      = buildLogDao();
            this.io       = buildIo();
            this.jiraffet = new Jiraffet(io.getNodeId(), log, io);

            LOG.debug("Starting node {}.", io.getNodeId());

            jiraffet.run();
        }
        catch (final Exception e) {
            LOG.error("Running Jiraffet.", e);
        }
    }
}
