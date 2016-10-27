package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class JiraffetTcpIO implements JiraffetIO {
    @Override
    public void requestVotes(RequestVoteRequest req) throws IOException {

    }

    @Override
    public void requestVotes(RequestVoteResponse req) throws IOException {

    }

    @Override
    public void appendEntries(String id, AppendEntriesRequest req) throws IOException {

    }

    @Override
    public void appendEntries(String id, AppendEntriesResponse resp) throws IOException {

    }

    @Override
    public int nodeCount() {
        return 0;
    }

    @Override
    public List<Message> getMessages(long timeout, TimeUnit timeunit) throws IOException, TimeoutException {
        return null;
    }

    @Override
    public List<String> nodes() {
        return null;
    }
}
