package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.util.AppConfiguration;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

/**
 * Tests related to {@link JiraffetDb} and its use of {@link AppConfiguration}.
 */
public class JiraffetDbConfigTest {

    @Test
    public void testHomeProperty() {
        final String expect = System.getProperties().getProperty("user.home") + "/.jiraffetdb";
        final AppConfiguration config = new AppConfiguration("jiraffetdb");
        final String actual = config.getString("jiraffetdb.home");
        assertEquals(expect, actual);
    }
}
