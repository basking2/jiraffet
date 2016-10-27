package com.github.basking2.jiraffet.db;

import org.junit.Test;

import java.sql.SQLException;

public class JiraffetDbTest {

    @Test
    public void testMigrate() throws SQLException, ClassNotFoundException {
            new JiraffetDb("test/db");

    }
}