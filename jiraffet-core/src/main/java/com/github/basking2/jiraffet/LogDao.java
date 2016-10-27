package com.github.basking2.jiraffet;

import java.io.IOException;

/**
 * The Raft Log entry.
 */
public interface LogDao {
    void setCurrentTerm(int currentTerm) throws IOException;
    int getCurrentTerm() throws IOException;

    void setVotedFor(String id) throws IOException;
    String getVotedFor() throws IOException;

    EntryMeta getMeta(int index) throws IOException;

    byte[] read(int index) throws IOException;

    boolean hasEntry(int index, int term) throws IOException;

    void write(int term, int index, byte[] data) throws IOException;

    /**
     * Remove this index, and all following entries.
     * @param i item index.
     * @throws IOException on any IO error.
     */
    void remove(int i) throws IOException;

    /**
     * Apply the given index to an internal state machine.
     * 
     * @param index The index of the command to apply to the internal log.
     * @throws IllegalStateException If the state is already applied.
     */
    void apply(int index) throws IllegalStateException;
    
    /**
     * Return the term of the last entry in the log.
     *
     * @throws IOException On any IO error.
     * @return The metadata for the last entry in the log. This should never be null.
     */
    EntryMeta last() throws IOException;

    class EntryMeta {
        private int term;
        private int index;

        public int getTerm() {
            return term;
        }

        public void setTerm(int term) {
            this.term = term;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }
    }

}
