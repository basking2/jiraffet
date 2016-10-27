package com.github.basking2.jiraffet.messages;

public class AppendEntriesResponse implements Message {
    private boolean success;
    private String from;
    private int nextCommitIndex;

    public AppendEntriesResponse() {
        this("", 0, false);
    }

    public AppendEntriesResponse(String from, int nextCommitIndex, boolean success) {
        this.success = success;
        this.from = from;
        this.nextCommitIndex = nextCommitIndex;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public int getNextCommitIndex() {
        return nextCommitIndex;
    }

    public void setNextCommitIndex(int nextCommitIndex) {
        this.nextCommitIndex = nextCommitIndex;
    }
}
