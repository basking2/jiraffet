package com.github.basking2.jiraffet;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.jiraffet.util.VersionVoter;

/**
 * Define how JiraffetRaft communicates with the outside world.
 *
 * This api is intended to function in a single threaded application, but may do it's work concurrently.
 */
public interface JiraffetIO
{
    /**
     * Send the given vote request to all nodes and collect the responses.
     *
     * @param req The JiraffetRaft vote request.
     * @return The responses.
     * @throws JiraffetIOException On any error.
     */
    List<RequestVoteResponse> requestVotes(RequestVoteRequest req) throws JiraffetIOException;

    /**
     * Append entries to all nodes.
     *
     * @param id The id we are sending to.
     * @param req The request.
     * @return The responses.
     * @throws JiraffetIOException On any IO exception.
     */
    List<AppendEntriesResponse> appendEntries(List<String> id, List<AppendEntriesRequest> req) throws JiraffetIOException;

    /**
     * Return this node's id on the network.
     * @return this node's id on the network.
     */
    String getNodeId();

    /**
     * Return the list of nodes this IO layer knows about. This makes up the cluster.
     * @return the list of nodes this IO layer knows about. This makes up the cluster.
     */
    List<String> nodes();

    int nodeCount();
}
