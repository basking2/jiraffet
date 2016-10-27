package com.github.basking2.jiraffet.db;

import com.github.basking2.jiraffet.LogDao;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.UUID;

import static org.junit.Assert.*;

public class LogDaoMyBatisTest {
    LogDaoMyBatis db;

    @Before public void setup() throws SQLException, ClassNotFoundException, IOException {
       db = new JiraffetDb("test/db").getLogDao();

       db.remove(Integer.MIN_VALUE);
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

        db.apply(Integer.MAX_VALUE);
        db.remove(Integer.MIN_VALUE);
        assertNull(db.getMeta(meta.getIndex()));
    }

    /*
    byte[] read(int index) throws IOException;
    void write(int term, int index, byte[] data) throws IOException;
    */

}
