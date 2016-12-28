package com.github.basking2.jiraffet;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.jiraffet.util.VersionVoter;

/**
 * Define how Jiraffet communicates with the outside world.
 *
 * This api is intended to function in a single threaded application, but may do it's work concurrently.
 */
public interface JiraffetIO extends VersionVoter.NodeCounter
{
    /**
     * Send the given vote request to all nodes and collect the responses.
     *
     * @param req The Jiraffet vote request.
     * @throws JiraffetIOException On any error.
     */
    void requestVotes(RequestVoteRequest req) throws JiraffetIOException;

    /**
     * Send the response.
     *
     * @param candidateId Candidate ID from the {@link RequestVoteRequest#getCandidateId()}.
     * @param req The request.
     * @throws JiraffetIOException On any IO error.
     */
    void requestVotes(String candidateId, RequestVoteResponse req) throws JiraffetIOException;

    /**
     * Append entries to all nodes.
     *
     * @param id The id we are sending to.
     * @param req The request.
     * @throws JiraffetIOException On any IO exception.
     */
    void appendEntries(String id, AppendEntriesRequest req) throws JiraffetIOException;

    /**
     * Send a response to a {@link #appendEntries(String, AppendEntriesRequest)}.
     *
     * @param id The id of the other node. This is always the current leader when the resp is successful.
     * @param resp The response.
     * @throws JiraffetIOException On any IO error.
     */
    void appendEntries(String id, AppendEntriesResponse resp) throws JiraffetIOException;

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
     * @throws InterruptedException If the waiting thread is interrupted.
     * @throws JiraffetIOException On any io error that means the leader can no longer function as the leader.
     *                     Throwing this should be very rare as it kills leadership.
     */
    List<Message> getMessages(long timeout, TimeUnit timeunit) throws JiraffetIOException, TimeoutException, InterruptedException;

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

    /**
     * This call relays a list of ClientRequests to the local instance of {@link Jiraffet}.
     *
     * It is expected that a client facade will call this and report back to the client
     * the status of the call to this local node.
     *
     * For example, a Servlet might be what calls this code, submitting a client HTTP POST as the
     * data and responding with a JSON encoding when {@link ClientRequest#complete(boolean, String, String)} is called.
     *
     * @param clientRequests A list of client requests to submit. This may, in practice, always be 1, but
     *                       as Raft allows for many client requests in a transaction, we offer that to the user.
     */
    void clientRequest(final List<ClientRequest> clientRequests);
}
