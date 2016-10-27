package com.github.basking2.jiraffet.db;

import org.junit.Test;

public class LogDaoDbManagerTest {

    @Test
    public void testMigrate() throws Exception {
        final EphemeralDirectory dir = new EphemeralDirectory();
        LogDaoDbManager db = new LogDaoDbManager(dir.getTemporaryDirectory().toString());
        db.close();
        dir.close();
    }
}