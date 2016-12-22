package com.github.basking2.otternet;

import com.github.basking2.jiraffet.Jiraffet;
import com.github.basking2.jiraffet.JiraffetTcpIO;
import com.github.basking2.jiraffet.JiraffetTcpIOFactory;
import com.github.basking2.jiraffet.db.LogDaoDbManager;
import com.github.basking2.jiraffet.db.LogDaoMyBatis;
import com.github.basking2.otternet.requests.AddNodeClientRequest;
import com.github.basking2.otternet.requests.ClientRequestResult;
import com.github.basking2.otternet.requests.RemoveNodeClientReqeuest;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Arrays.asList;

public class OtterNet {
    final private JiraffetTcpIO io;
    final private LogDaoDbManager dbManager;
    final private LogDaoMyBatis logDao;
    final private Jiraffet jiraffet;

    public OtterNet() throws IOException, SQLException, ClassNotFoundException {
        this.io = JiraffetTcpIOFactory.buildIo("0.0.0.0:18079", new String[]{});
        this.dbManager = new LogDaoDbManager("dir");
        this.logDao = dbManager.getLogDao();
        this.jiraffet = new Jiraffet(logDao, io);
    }

    public static final void main(final String[] argv) {

    }

    public ClientRequestResult addNode(final String node) throws InterruptedException, ExecutionException, TimeoutException {

        // Check if the node is already in our set.
        for (final String id : io.nodes()) {
            if (id.equals(node)) {
                return new ClientRequestResult(true, jiraffet.getCurrentLeader(), "Node is already in our set.");
            }
        }

        final AddNodeClientRequest addNode = new AddNodeClientRequest(node);

        io.clientRequest(asList(addNode));

        final ClientRequestResult r = addNode.getFuture().get(1, TimeUnit.MINUTES);

        // After the config is accepted, move forwards.
        io.nodes().add(node);

        return r;
    }

    public ClientRequestResult removeNode(final String node) throws InterruptedException, ExecutionException, TimeoutException {

        final RemoveNodeClientReqeuest remNode = new RemoveNodeClientReqeuest(node);

        io.clientRequest(asList(remNode));

        final ClientRequestResult r = remNode.getFuture().get(1, TimeUnit.MINUTES);

        // After the config is accepted, move forwards.
        io.nodes().remove(node);

        if (node.equals(io.getNodeId())) {
            jiraffet.shutdown();
        }

        return r;
    }
}