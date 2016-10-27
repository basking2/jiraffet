package com.github.basking2.jiraffet;

import org.apache.commons.configuration2.ex.ConfigurationException;

import java.io.IOException;
import java.sql.SQLException;

public class JiraffetDbMain {

    public static void main(final String[] args) throws SQLException, IOException, ClassNotFoundException, ConfigurationException {

        JiraffetDb app = new JiraffetDb();
        app.start();

    }
}