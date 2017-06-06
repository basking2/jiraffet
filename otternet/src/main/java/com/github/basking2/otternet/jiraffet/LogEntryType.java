package com.github.basking2.otternet.jiraffet;

/**
 */
public enum LogEntryType {
    /**
     * A log entry that has no effect. Useful for setting barrier versions.
     */
    NOP_ENTRY,

    /**
     * A node is added to the cluster. Contents is a string.
     */
    JOIN_ENTRY,
    /**
     * A node is removed from the cluster. Contents is a string.
     */
    LEAVE_ENTRY,

    /**
     * This signals that the log should be compacted from the start up to the encoded integer index.
     */
    SNAPSHOT_ENTRY,

    /**
     * A named sequence of bytes.
     *
     * This is encoded with the 1byte type, then the key length, then the key in bytes.
     * Then the type length, and the type in bytes.
     * Then the data length, and the data in bytes.
     */
    BLOB_ENTRY;

    public static LogEntryType fromByte(byte b) {
        if (b < 0 || b >= values().length) {
            return NOP_ENTRY;
        }

        return values()[b];
    }
}
