package com.github.basking2.otternet.requests;

/**
 */
public class ClientRequestResult {
    final private boolean success;
    final private String leader;
    final private String msg;

    public ClientRequestResult(boolean success, String leader, String msg) {
        this.success = success;
        this.leader = leader;
        this.msg = msg;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getLeader() {
        return leader;
    }

    public String getMsg() {
        return msg;
    }
}
