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
}
