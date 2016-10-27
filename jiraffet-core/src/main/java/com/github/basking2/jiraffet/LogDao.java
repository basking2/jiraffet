package com.github.basking2.jiraffet;

import java.io.IOException;

/**
 * The Raft Log entry.
 */
public interface LogDao {

    /**
     * Set the current term. This is distinct from the term of the last log entry.
     *
     * @param currentTerm The current term.
     * @throws IOException On any error.
     */
    void setCurrentTerm(int currentTerm) throws IOException;

    /**
     * Get the current term. This is distinct from teh term of the last log entry.
     *
     * @return The current term.
     * @throws IOException On any error.
     */
    int getCurrentTerm() throws IOException;

    /**
     * Set who we voted for.
     *
     * @param id The id of who we voted for.
     * @throws IOException On any error.
     */
    void setVotedFor(String id) throws IOException;

    /**
     * @return Who we voted for. This should be reset when a new leader establishes themselves / the term changes.
     * @throws IOException On any error.
     */
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
