package com.github.basking2.jiraffet.db;

/**
 */
public class KeyValueEntry {
    private String id;
    private String type;
    private byte[] data;

    public KeyValueEntry(final String id, final String type, final byte[] data) {
        this.id = id;
        this.type = type;
        this.data = data;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
