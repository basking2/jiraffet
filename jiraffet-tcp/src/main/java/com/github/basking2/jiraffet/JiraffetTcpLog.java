package com.github.basking2.jiraffet;

import java.io.IOException;

/**
 */
public class JiraffetTcpLog implements LogDao {
    @Override
    public void setCurrentTerm(int currentTerm) throws IOException {

    }

    @Override
    public int getCurrentTerm() throws IOException {
        return 0;
    }

    @Override
    public void setVotedFor(String id) throws IOException {

    }

    @Override
    public String getVotedFor() throws IOException {
        return null;
    }

    @Override
    public EntryMeta getMeta(int index) throws IOException {
        return null;
    }

    @Override
    public byte[] read(int index) throws IOException {
        return new byte[0];
    }

    @Override
    public boolean hasEntry(int index, int term) throws IOException {
        return false;
    }

    @Override
    public void write(int term, int index, byte[] data) throws IOException {

    }

    @Override
    public void remove(int i) throws IOException {

    }

    @Override
    public void apply(int index) throws IllegalStateException {

    }

    @Override
    public EntryMeta last() throws IOException {
        return null;
    }
}
