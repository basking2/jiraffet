package com.github.basking2.otternet.jiraffet;

import java.util.HashMap;
import java.util.Map;

/**
 * How Otter stores key/values.
 */
public class KeyValueStore {
    private Map<String, Blob> map = new HashMap<>();

    public void put(final String key, final Blob value) {
        synchronized (map) {
            map.put(key, value);
        }
    }

    public Blob get(final String key) {
        synchronized (map) {
            return map.get(key);
        }
    }

    public static class Blob {
        private byte[] data;
        private String type;

        public Blob(final String type, final byte[] data) {
            this.data = data;
            this.type = type;
        }

        public byte[] getData() {
            return data;
        }

        public void setData(byte[] data) {
            this.data = data;
        }

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }
    }
}
