package com.github.basking2.jiraffetdb.util;

/**
 */
public class KeyValue {
    private String key;
    private byte[] value;

    public KeyValue(final String key, final byte[] value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }
}
