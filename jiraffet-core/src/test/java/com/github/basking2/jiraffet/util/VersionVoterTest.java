package com.github.basking2.jiraffet.util;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class VersionVoterTest {

    @Test
    public void testVoteFor() {
        final VersionVoter vv = new VersionVoter(5);
        
        final int[] votes = new int[] { 0 };
        
        vv.setListener(3, (ver, succ) -> {
            votes[0] = ver;
        });
        
        assertEquals(0, votes[0]);
        vv.vote(2);
        vv.vote(2);
        vv.vote(2);
        vv.vote(2);
        assertEquals(0, votes[0]);
        vv.vote(3);
        vv.vote(3);
        vv.vote(3);
        assertEquals(3, votes[0]);
        assertEquals(3, vv.getCurrentVersion());
    }

    @Test
    public void testVoteForHigher() {
        final VersionVoter vv = new VersionVoter(5);
        
        final int[] votes = new int[] { 0 };
        
        vv.setListener(3, (ver, succ) -> {
            votes[0] = ver;
        });
        
        assertEquals(0, votes[0]);
        vv.vote(3);
        vv.vote(2);
        vv.vote(4);
        vv.vote(4);
        vv.vote(4);
        assertEquals(3, votes[0]);
        assertEquals(4, vv.getCurrentVersion());
    }

}
