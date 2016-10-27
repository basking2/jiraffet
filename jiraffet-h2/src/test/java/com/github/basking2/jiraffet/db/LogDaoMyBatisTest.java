package com.github.basking2.jiraffet.db;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.sql.SQLException;
import java.util.UUID;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.basking2.jiraffet.LogDao;

public class LogDaoMyBatisTest {
    static EphemeralDirectory dir;
    static LogDaoDbManager database;
    static LogDaoMyBatis db;

    @Before public void setup() throws SQLException, ClassNotFoundException, IOException {
       db.remove(Integer.MIN_VALUE);
    }
    
    @After public void teardown() throws Exception {
    }
    
    @BeforeClass public static void startup() throws IOException, ClassNotFoundException, SQLException {
        dir = new EphemeralDirectory();
        database = new LogDaoDbManager(dir.getTemporaryDirectory().toString());
        db = database.getLogDao();
    }
    @AfterClass public static void cleanup() throws Exception {
        database.close();
        dir.close();
    }

    @Test public void testSetCurrentTerm() throws IOException {
        int term = (int)(Math.random() * Integer.MAX_VALUE);
        db.setCurrentTerm(term);
        assertEquals(term, db.getCurrentTerm());
    }

    @Test public void testSetVotedFor() throws IOException {
        final String votedFor = UUID.randomUUID().toString();
        db.setVotedFor(votedFor);
        assertEquals(votedFor, db.getVotedFor());
    }

    @Test public void testGetMeta() throws IOException {
        LogDao.EntryMeta meta = new LogDao.EntryMeta(
                (int)(Math.random() * Integer.MAX_VALUE),
                (int)(Math.random() * Integer.MAX_VALUE));

        assertNull(db.getMeta(meta.getIndex()));

        db.write(meta.getTerm(), meta.getIndex(), new byte[]{1,2,3,4});

        assertNotNull(db.getMeta(meta.getIndex()));
        assertEquals(meta.getIndex(), db.getMeta(meta.getIndex()).getIndex());
        assertEquals(meta.getTerm(), db.getMeta(meta.getIndex()).getTerm());

        assertEquals(meta.getIndex(), db.last().getIndex());
        assertEquals(meta.getTerm(), db.last().getTerm());

        assertTrue(db.hasEntry(meta.getIndex(), meta.getTerm()));
        assertFalse(db.hasEntry(meta.getIndex()+1, meta.getTerm()));
        assertFalse(db.hasEntry(meta.getIndex(), meta.getTerm()+1));

        db.remove(Integer.MIN_VALUE);
        assertNull(db.getMeta(meta.getIndex()));
    }
    
    @Test public void testRead() throws IOException {
        final byte[] data = new byte[]{ 1, 2, 3, 4 };
        db.write(2, 1, data);
        assertArrayEquals(data, db.read(1));
    }
}
