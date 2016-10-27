package com.github.basking2.jiraffet;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.Message;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;

/**
 * Define how Jiraffet communicates with the outside world.
 *
 * This api is intended to function in a single threaded application, but may do it's work concurrently.
 */
public interface JiraffetIO
{
    /**
     * Send the given vote request to all nodes and collect the responses.
     *
     * @param req The Jiraffet vote request.
     * @throws IOException On any error.
     */
    void requestVotes(RequestVoteRequest req) throws IOException;

    /**
     * Send the response.
     *
     * @param req The request.
     * @throws IOException On any IO error.
     */
    void requestVotes(RequestVoteResponse req) throws IOException;

    /**
     * Append entries to all nodes.
     *
     * @param id The id we are sending to.
     * @param req The request.
     * @throws IOException On any IO exception.
     */
    void appendEntries(String id, AppendEntriesRequest req) throws IOException;

    /**
     * Return the number of nodes this object knows about.
     *
     * Quorum is ((nodeCount() + 1) / 2) + 1.
     *
     * @return the number of nodes this object knows about.
     */
    int nodeCount();

    /**
     * Read from the input queue as many messages as are available.
     *
     * If 0 messages are returned it is assumed we've timed out.
     *
     * @param timeout The timeout to wait for a response.
     * @param timeunit The timeunit of {@code timeout}.
     *
     * @return list of messages from the IO layer. If 0 messages are returned then we act as though we've timed out.
     * @throws TimeoutException When 1/2 the heartbeat interval has passed and no messages have been received.
     * @throws IOException On any io error that means the leader can no longer function as the leader.
     *                     Throwing this should be very rare as it kills leadership.
     */
    List<Message> getMessages(long timeout, TimeUnit timeunit) throws IOException, TimeoutException;

    /**
     * Return the list of nodes this IO layer knows about. This makes up the cluster.
     * @return the list of nodes this IO layer knows about. This makes up the cluster.
     */
    List<String> nodes();
}
