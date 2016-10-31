package com.github.basking2.jiraffetdb;

import org.apache.commons.configuration2.ex.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;

public class JiraffetDbMain {

    public static final Logger LOG = LoggerFactory.getLogger(JiraffetDbMain.class);

    public static void main(final String[] args) throws SQLException, IOException, ClassNotFoundException, ConfigurationException {

        LOG.debug("Application starting.");
        JiraffetDb app = new JiraffetDb();
        app.start();

    }
}