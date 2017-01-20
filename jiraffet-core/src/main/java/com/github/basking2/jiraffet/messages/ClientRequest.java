package com.github.basking2.jiraffet.messages;

/**
 * This is a request from an entity outside of the Raft algorithm.
 */
public interface ClientRequest extends Message {
    /**
     * @return the data the client is requesting be stored.
     */
    byte[] getData();


    /**
     * JiraffetRaft will report back the result of the request through this interface.
     *
     * @param success True if successful.
     * @param leader The leader ID, if known and this node is not the leader.
     * @param msg Error message if success is false. Optional. May be null.
     *
     */
    void complete(boolean success, String leader, String msg);
}
