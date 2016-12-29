package com.github.basking2.jiraffet;

/**
 * The Raft Log entry.
 */
public interface LogDao {

    /**
     * Set the current term. This is distinct from the term of the last log entry.
     *
     * @param currentTerm The current term.
     * @throws JiraffetIOException On any error.
     */
    void setCurrentTerm(int currentTerm) throws JiraffetIOException;

    /**
     * Get the current term. This is distinct from teh term of the last log entry.
     *
     * @return The current term.
     * @throws JiraffetIOException On any error.
     */
    int getCurrentTerm() throws JiraffetIOException;

    /**
     * Set who we voted for.
     *
     * @param id The id of who we voted for.
     * @throws JiraffetIOException On any error.
     */
    void setVotedFor(String id) throws JiraffetIOException;

    /**
     * @return Who we voted for. This should be reset when a new leader establishes themselves / the term changes.
     * @throws JiraffetIOException On any error.
     */
    String getVotedFor() throws JiraffetIOException;

    EntryMeta getMeta(int index) throws JiraffetIOException;

    /**
     * Read a log entry.
     *
     * @param index The index of the entry to fetch.
     * @return A log entry.
     * @throws JiraffetIOException On a read error.
     */
    byte[] read(int index) throws JiraffetIOException;

    boolean hasEntry(int index, int term) throws JiraffetIOException;

    void write(int term, int index, byte[] data) throws JiraffetIOException;

    /**
     * Remove this index, and all following entries.
     * @param i item index.
     * @throws JiraffetIOException on any IO error.
     */
    void remove(int i) throws JiraffetIOException;

    /**
     * Apply the given index to an internal state machine.
     * 
     * @param index The index of the command to apply to the internal log.
     * @throws IllegalStateException If the state is already applied.
     */
    void apply(int index) throws IllegalStateException;

    /**
     * Return the index of the last index that was applied.
     *
     * @return
     */
    int lastApplied();
    
    /**
     * Return the term of the last entry in the log.
     *
     * @throws JiraffetIOException On any IO error.
     * @return The metadata for the last entry in the log. This should never be null.
     */
    EntryMeta last() throws JiraffetIOException;

    class EntryMeta {
        private int term;
        private int index;

        public EntryMeta(Integer term, Integer index){
            this.term = term;
            this.index = index;
        }

        public EntryMeta() {
            this(0,0);
        }

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
