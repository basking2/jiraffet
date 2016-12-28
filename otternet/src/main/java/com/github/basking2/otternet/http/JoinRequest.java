package com.github.basking2.otternet.http;

/**
 */
public class JoinRequest {
    private String id;

    public JoinRequest(final String id) {
        this.id = id;
    }

    public JoinRequest() {
        this(null);
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
