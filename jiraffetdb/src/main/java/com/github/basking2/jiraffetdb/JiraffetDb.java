package com.github.basking2.jiraffetdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.SQLException;

import com.github.basking2.jiraffet.Jiraffet;
import com.github.basking2.jiraffet.JiraffetTcpIO;
import com.github.basking2.jiraffet.JiraffetTcpIOFactory;
import com.github.basking2.jiraffetdb.dao.DbManager;
import com.github.basking2.jiraffetdb.dao.KeyValueDao;
import com.github.basking2.jiraffet.db.LogDaoMyBatis;
import com.github.basking2.jiraffetdb.util.KeyValueProtocol;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.db.LogDaoDbManager;
import com.github.basking2.jiraffetdb.util.App;

/**
 * How to configure the TCP app.
 */
public class JiraffetDb {
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetDb.class);

    private Configuration appConfiguration;
    private LogDaoMyBatis log;
    private KeyValueDao kv;
    private JiraffetTcpIO io;

    public JiraffetDb() throws ConfigurationException, SQLException, ClassNotFoundException, IOException {
        this.appConfiguration = new App(this.getClass()).buildConfiguration();

        this.kv = buildKvDao();
        this.log = buildLogDao();
        this.io = buildIo();

        this.log.addApplier(index -> {
            final ByteBuffer bb = ByteBuffer.wrap(log.read(index));

            kv.set(KeyValueProtocol.unmarshal(bb), index);
        });
    }

    private JiraffetTcpIO buildIo() throws IOException {
        final String thisNode = appConfiguration.getString("jiraffetdb.this.node").trim();
        final String[] nodesProp = appConfiguration.getString("jiraffetdb.nodes").split(",");

        final JiraffetTcpIO io = JiraffetTcpIOFactory.buildIo(thisNode, nodesProp);

        return io;
    }

    public KeyValueDao buildKvDao() throws SQLException, ClassNotFoundException {
        String dbHome = appConfiguration.getString("jiraffetdb.kv.db");

        if (dbHome == null) {
            dbHome = appConfiguration.getString("jiraffetdb.home") + "/kvdb";
        }

        final DbManager db = new DbManager(dbHome);

        return db.getLogDao();

    }

    public LogDaoMyBatis buildLogDao() throws SQLException, ClassNotFoundException {
        String dbHome = appConfiguration.getString("jiraffetdb.raft.db");

        if (dbHome == null) {
            dbHome = appConfiguration.getString("jiraffetdb.home") + "/raftdb";
        }

        final LogDaoDbManager db = new LogDaoDbManager(dbHome);

        return db.getLogDao();
    }


    public void start() {
        try {
            Jiraffet jiraffet = new Jiraffet(io.getNodeId(), log, io);

            jiraffet.setLeaderTimeout(2000);
            jiraffet.setFollowerTimeout(5000);

            LOG.debug("Starting node {}.", io.getNodeId());

            jiraffet.run();
        }
        catch (final Exception e) {
            LOG.error("Running Jiraffet.", e);
        }
    }
}
