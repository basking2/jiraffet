package com.github.basking2.jiraffet.messages;

/**
 * The Log Entry from the user into the log.
 *
 * This consists of the term, the index and the data.
 */
public class LogEntry {
    private int index;
    private int term;
    private byte[] data;

    public LogEntry(final int index, final int term, final byte[] data) {
        this.index = index;
        this.term = term;
        this.data = data;
    }

    public LogEntry() {
        this(0, 0, new byte[]{});
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }
}
