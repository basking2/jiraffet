package com.github.basking2.jiraffet.messages;

/**
 */
public class RequestVoteResponse implements Message {
    /**
     * The term the vote is for.
     */
    private int term;

    /**
     * The vote is granted or not.
     */
    private boolean voteGranted;

    public RequestVoteResponse() {
        this(0, false);
    }

    public RequestVoteResponse(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }
}
