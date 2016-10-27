package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.util.AppConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * How to configure the TCP app.
 */
public class JiraffetTcpApp {
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetTcpApp.class);

    private AppConfiguration appConfiguration;
    private Jiraffet jiraffet;
    private LogDao log;
    private JiraffetIO io;

    public JiraffetTcpApp() {
        this.appConfiguration = new AppConfiguration("jiraffet");
    }

    public void start() {
        LOG.debug("Starting.");

        this.log = null; // FIXME
        this.io = null; // FIXME

        this.jiraffet = new Jiraffet(log, io);

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
