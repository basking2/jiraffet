package com.github.basking2.otternet.http;

/**
 * When a node tries to join an otter raft, this is the response.
 */
public class JoinResponse extends JsonResponse {
    /**
     * This is the zero-value for the transaction log.
     *
     * The client should request this index to start replicating the log.
     */
    private int logCompactionIndex;

    private String leader;

    private int term;

    private String logId;

    public int getLogCompactionIndex() {
        return logCompactionIndex;
    }

    public void setLogCompactionIndex(int logCompactionIndex) {
        this.logCompactionIndex = logCompactionIndex;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getLogId() {
        return logId;
    }

    public void setLogId(String logId) {
        this.logId = logId;
    }
}
