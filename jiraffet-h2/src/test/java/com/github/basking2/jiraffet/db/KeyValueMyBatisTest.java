package com.github.basking2.jiraffet.db;

import com.github.basking2.jiraffet.JiraffetIOException;
import org.junit.*;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

/**
 */
public class KeyValueMyBatisTest {
    static EphemeralDirectory dir;
    static LogDbManager database;
    static KeyValueMyBatis db;

    @Before
    public void setup() throws SQLException, ClassNotFoundException, JiraffetIOException {
        db.clear();
    }

    @After
    public void teardown() throws Exception {
    }

    @BeforeClass
    public static void startup() throws IOException, JiraffetIOException, ClassNotFoundException, SQLException {
        dir = new EphemeralDirectory();
        database = new LogDbManager(dir.getTemporaryDirectory().toString());
        db = database.getKeyValue();
    }

    @AfterClass
    public static void cleanup() throws Exception {
        database.close();
        dir.close();
    }

    @Test
    public void test() throws JiraffetIOException {
        db.put("a", "a".getBytes());
        assertEquals("a", new String(db.get("a")));
        db.delete("a");
        assertEquals(null, db.get("a"));
    }

    @Test
    public void testEntry() throws JiraffetIOException {
        KeyValueEntry e = new KeyValueEntry("a", "atype", "a".getBytes());
        db.putEntry(e);
        KeyValueEntry e2 = db.getEntry(e.getId());

        assertEquals(e.getId(), e2.getId());
        assertEquals(e.getType(), e2.getType());

    }
}
