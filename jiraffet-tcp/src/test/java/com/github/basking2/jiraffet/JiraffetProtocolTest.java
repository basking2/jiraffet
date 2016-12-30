package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;
import org.junit.Assert;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class JiraffetProtocolTest {
    final JiraffetProtocol jiraffetProtocol = new JiraffetProtocol();

    @Test
    public void testRequestVoteRequest() {
        final RequestVoteRequest msg = new RequestVoteRequest(1, "myid", 2, 3);
        final ByteBuffer header = jiraffetProtocol.marshal(msg);
        header.position(8);
        final ByteBuffer data = header.slice();
        header.position(0);

        final RequestVoteRequest msg2 = (RequestVoteRequest) jiraffetProtocol.unmarshal(header, data);


        assertEquals(msg.getLastLogIndex(), msg2.getLastLogIndex());
        assertEquals(msg.getCandidateId(), msg2.getCandidateId());
        assertEquals(msg.getLastLogTerm(), msg2.getLastLogTerm());
        assertEquals(msg.getTerm(), msg2.getTerm());
    }

    @Test
    public void testRequestVoteResponse() {
        final RequestVoteResponse msg = new RequestVoteResponse(3, true);
        final ByteBuffer header = jiraffetProtocol.marshal(msg);
        header.position(8);
        final ByteBuffer data = header.slice();
        header.position(0);
        final RequestVoteResponse msg2 = (RequestVoteResponse) jiraffetProtocol.unmarshal(header, data);

        assertEquals(msg.getTerm(), msg2.getTerm());
        assertEquals(msg.isVoteGranted(), msg2.isVoteGranted());
    }

    @Test
    public void testAppendEntriesResponse() {
        final AppendEntriesResponse msg = new AppendEntriesResponse("hi", 1, true);
        final ByteBuffer header = jiraffetProtocol.marshal(msg);
        header.position(8);
        final ByteBuffer data = header.slice();
        header.position(0);
        final AppendEntriesResponse msg2 = (AppendEntriesResponse) jiraffetProtocol.unmarshal(header, data);

        assertEquals(msg.getFrom(), msg2.getFrom());
        assertEquals(msg.isSuccess(), msg2.isSuccess());
        assertEquals(msg.getNextCommitIndex(), msg2.getNextCommitIndex());
    }

    @Test
    public void testAppendEntriesRequest() {
        final List<byte[]> entries = new ArrayList<byte[]>();
        entries.add(new byte[]{1,2,3,4});
        entries.add(new byte[]{4,3,2,1});
        final AppendEntriesRequest msg = new AppendEntriesRequest(10, "myid", 4, 3, entries, 4);
        final ByteBuffer header = jiraffetProtocol.marshal(msg);
        header.position(8);
        final ByteBuffer data = header.slice();
        header.position(0);
        final AppendEntriesRequest msg2 = (AppendEntriesRequest) jiraffetProtocol.unmarshal(header, data);

        assertEquals(msg.getTerm(), msg2.getTerm());
        assertEquals(msg.getLeaderCommit(), msg2.getLeaderCommit());
        assertEquals(msg.getLeaderId(), msg2.getLeaderId());
        assertEquals(msg.getPrevLogIndex(), msg2.getPrevLogIndex());
        assertEquals(msg.getPrevLogTerm(), msg2.getPrevLogTerm());

        for (int i = 0; i < entries.size(); ++i){
            Assert.assertArrayEquals(entries.get(i), msg2.getEntries().get(i));
        }
    }
}
