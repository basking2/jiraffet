package com.github.basking2.jiraffet.messages;

import java.util.ArrayList;
import java.util.List;

import com.github.basking2.jiraffet.LogDao;

public class AppendEntriesRequest implements Message {
    private int term;
    private String leaderId;
    private int prevLogIndex;
    private int prevLogTerm;
    private List<byte[]> entries;
    private int leaderCommit;

    public AppendEntriesRequest() {
        this(0, "", 0, 0, new ArrayList<>(0), 0);
    }

    public AppendEntriesRequest(
            int term,
            String leaderId,
            LogDao.EntryMeta prevLogTerm,
            List<byte[]> entries,
            int leaderCommit
    ) {
        this(term, leaderId, prevLogTerm.getIndex(), prevLogTerm.getTerm(), entries, leaderCommit);
    }

    public AppendEntriesRequest(
            int term,
            String leaderId,
            int prevLogIndex,
            int prevLogTerm,
            List<byte[]> entries,
            int leaderCommit
    ) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.entries = entries;
        this.leaderCommit = leaderCommit;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public List<byte[]> getEntries() {
        return entries;
    }

    public void setEntries(List<byte[]> entries) {
        this.entries = entries;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    /**
     * Reject the request citing the new term.
     *
     * @param from The id of the node rejecting the request.
     * @param nextCommitIndex The next commit index this node expects to receive.
     * @return The response.
     */
    public AppendEntriesResponse reject(String from, int nextCommitIndex) {
        return new AppendEntriesResponse(from, nextCommitIndex, false);
    }

    /**
     * Accept the response with the next index set to prevLogIndex + entries + 1.
     *
     * @param from The id of the system responding.
     * @param nextCommitIndex The next index this node wants.
     * @return The response.
     */
    public AppendEntriesResponse accept(final String from, final int nextCommitIndex) {
        return new AppendEntriesResponse(from, nextCommitIndex, false);
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }
}
