package com.github.basking2.jiraffet;

import java.io.IOException;
import java.sql.SQLException;

public class JiraffetDbMain {

    public static void main(final String[] args) throws SQLException, IOException, ClassNotFoundException {

        JiraffetDb app = new JiraffetDb();
        app.start();

    }
}