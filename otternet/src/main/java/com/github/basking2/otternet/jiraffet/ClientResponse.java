package com.github.basking2.otternet.jiraffet;

/**
 * Data class used to capture the results of a client request.
 */
public class ClientResponse {
    private boolean success;
    private String leader;
    private String message;

    public ClientResponse(final boolean success, final String leader, final String message) {
        this.success = success;
        this.leader = leader;
        this.message = message;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getLeader() {
        return leader;
    }

    public void setLeader(String leader) {
        this.leader = leader;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
