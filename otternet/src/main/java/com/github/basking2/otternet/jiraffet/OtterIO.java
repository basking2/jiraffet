package com.github.basking2.otternet.jiraffet;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.basking2.jiraffet.JiraffetIO;
import com.github.basking2.jiraffet.JiraffetIOException;
import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.ClientRequest;
import com.github.basking2.jiraffet.messages.Message;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;

public class OtterIO implements JiraffetIO {
    
    private List<String> nodes;
    private String nodeId;
    
    public OtterIO(
            final String nodeId,
            final List<String> nodes
    ) {
        this.nodeId = nodeId;
        this.nodes = nodes;
    }

    @Override
    public int nodeCount() {
        return nodes.size();
    }

    @Override
    public void requestVotes(RequestVoteRequest req) throws JiraffetIOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void requestVotes(String candidateId, RequestVoteResponse req) throws JiraffetIOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void appendEntries(String id, AppendEntriesRequest req) throws JiraffetIOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void appendEntries(String id, AppendEntriesResponse resp) throws JiraffetIOException {
        // TODO Auto-generated method stub
        
    }

    @Override
    public List<Message> getMessages(long timeout, TimeUnit timeunit)
            throws JiraffetIOException, TimeoutException, InterruptedException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getNodeId() {
        return nodeId;
    }

    @Override
    public List<String> nodes() {
        return nodes;
    }

    @Override
    public void clientRequest(List<ClientRequest> clientRequests) {
        // TODO Auto-generated method stub
        
    }

}
