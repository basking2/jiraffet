package com.github.basking2.jiraffet.messages;

import com.github.basking2.jiraffet.JiraffetLog;

/**
 * Created by sskinger on 10/10/16.
 */
public class RequestVoteRequest implements Message {
    private int term;
    private String candidateId;
    private int lastLogIndex;
    private int lastLogTerm;

    public RequestVoteRequest() {
        this(0, "", 0, 0);
    }

    public RequestVoteRequest(int term, String candidateId, JiraffetLog.EntryMeta meta) {
        this(term, candidateId, meta.getIndex(), meta.getTerm());
    }

    public RequestVoteRequest(int term, String candidateId, int lastLogIndex, int lastLogTerm)
    {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    /**
     * @return A false vote to be sent to the requesting candidate.
     */
    public RequestVoteResponse reject()
    {
        return new RequestVoteResponse(term, false);
    }

    /**
     * @return A vote for the candidates to be sent to the requesting candidate.
     */
    public RequestVoteResponse vote()
    {
        return new RequestVoteResponse(term, true);
    }
}
