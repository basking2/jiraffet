package com.github.basking2.jiraffet.db;

import org.junit.Test;

public class JiraffetDbTest {

    @Test
    public void testMigrate() throws Exception {
        final EphemeralDirectory dir = new EphemeralDirectory();
        JiraffetDb db = new JiraffetDb(dir.getTemporaryDirectory().toString());
        db.close();
        dir.close();
    }
}