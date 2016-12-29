package com.github.basking2.jiraffet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import com.github.basking2.jiraffet.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.LogDao.EntryMeta;
import com.github.basking2.jiraffet.util.Timer;

/**
 * An instance of the Raft algorithm.
 */
public class Jiraffet
{
    public static final Logger LOG = LoggerFactory.getLogger(Jiraffet.class);

    /**
     * A limit on the number of log messages sent in any update.
     */
    private static final int LOG_ENTRY_LIMIT = 10;

    private String currentLeader;

    /**
     * The log the Raft algorithm is managing..
     */
    private LogDao log;

    /**
     * Map of what we think the followers need for their next index.
     */
    private Map<String, Integer> nextIndex;

    /**
     * Are we a leader, follower, or candidate.
     */
    private State mode;

    /**
     * Number of votes we have.
     */
    private int votes;

    /**
     * How we talk to others.
     */
    private JiraffetIO io;

    /**
     * How long should the algorithm wait for messages.
     *
     * If messages are received before this, it is an algorithmic choice what the timer value should be.
     * Should it decrease or be reset?
     */
    private Timer receiveTimer;

    /**
     * Is the service running?
     */
    private volatile boolean running;

    enum State {
        CANDIDATE,
        FOLLOWER,
        LEADER
    };

    /**
     * Timeout after which a follower will seek to be elected.
     */
    private long followerTimeoutMs;

    /**
     * Timeout after which a leader should send a heartbeat to prevent an election.
     */
    private long leaderTimeoutMs;

    /**
     * @param log Where entries are committed and applied. Also it holds some persistent state such
     *            as the current term and whom we last voted for.
     * @param io How messages are sent to other nodes.
     */
    public Jiraffet(final LogDao log, final JiraffetIO io) {
        this.io = io;
        this.log = log;
        this.mode = State.FOLLOWER;
        this.nextIndex = new HashMap<>();
        this.running = false;
        this.leaderTimeoutMs = 5000;
        this.followerTimeoutMs = 4 * this.leaderTimeoutMs;
        this.receiveTimer = new Timer(followerTimeoutMs);
    }

    /**
     * Set the timeout for the leader nodes. This should be somehwat shorter.
     * 
     * @see #setFollowerTimeout(long)
     * 
     * @param timeout The timeout in milliseconds.
     */
    public void setLeaderTimeout(final long timeout) {
        this.leaderTimeoutMs = timeout;
    }
    
    /**
     * Set the follower timeout. This should be twice the size of the leader timeout.
     * 
     * @see #setLeaderTimeout(long)
     * 
     * @param timeout The timeout in milliseconds.
     */
    public void setFollowerTimeout(final long timeout) {
        this.followerTimeoutMs = timeout;
    }

    /**
     * Invoked by leader to replicate log entries; also used as heartbeat.
     *
     * This creates a request that should be sent to followers.
     *
     * @param id The ID of the node to send messages to.
     * @return Request to send to followers.
     * @throws JiraffetIOException Any IO error.
     */
    public AppendEntriesRequest appendEntries(final String id) throws JiraffetIOException {

        // A node has not told the leader which index they want next.
        // Assume they would like the very next index (they are totally up-to-date).
        // If they are not, they can correct us.
        if (!nextIndex.containsKey(id)) {
            nextIndex.put(id, log.last().getIndex()+1);
        }

        // Get the most data we have and will try to commit.
        final int largestIndex = log.last().getIndex();

        // Get the commit log index we would like to send to the node.
        final int nextExpectedIndex = nextIndex.get(id);

        final List<byte[]> entries;

        // If we have fewer entries than another node then the node is up-to-date.
        // That is, it is expecting a log yet to be created or it has un-applied entries we will replace.
        if (largestIndex < nextExpectedIndex) {
            entries = new ArrayList<>();
        }

        // Otherwise, the node needs some records.
        else {

            final int entriesToSend = Math.min(LOG_ENTRY_LIMIT, largestIndex - nextExpectedIndex + 1);

            entries = new ArrayList<>(entriesToSend);

            for (int i = 0; i < entriesToSend; ++i) {
                entries.add(log.read(nextExpectedIndex + i));
            }
        }

        return new AppendEntriesRequest(
                log.getCurrentTerm(),
                currentLeader,
                log.getMeta(nextExpectedIndex-1),
                entries,
                log.last().getIndex());
    }

    /**
     * Set a new node as a leader.
     *
     * This is very useful when programatically changing cluster membership.
     */
    public void setNewLeader(final String leaderId, int term) throws JiraffetIOException {
        LOG.info("New leader {}.", leaderId);
        log.setVotedFor(null);
        log.setCurrentTerm(term);
        mode = State.FOLLOWER;
        currentLeader = leaderId;
        receiveTimer.set(followerTimeoutMs);
    }

    /**
     * Invoked by leader to replicate log entries; also used as heartbeat.
     *
     * This is executed on the follower when an AppendEntriesRequest is received.
     *
     * @param req The request.
     * @returns The response.
     * @throws JiraffetIOException Any IO error.
     */
    public AppendEntriesResponse appendEntries(final AppendEntriesRequest req) throws JiraffetIOException {

        final AppendEntriesResponse resp;

        // If the leader and term match, try to do this. Reset the timeout to the full value.
        if (req.getTerm() < log.getCurrentTerm()) {
            LOG.info("Rejecting log from {}. Previous term {}.", req.getLeaderId(), req.getPrevLogTerm());
            // Ignore invalid request.
            resp = req.reject(io.getNodeId(), log.last().getIndex()+1);
        }

        // If the leader knows the previous log state, we can apply this.
        else if (log.hasEntry(req.getPrevLogIndex(), req.getPrevLogTerm())) {

            // If the term is greater, we have a new leader. Adjust things.
            if (req.getTerm() > log.getCurrentTerm()) {
                setNewLeader(req.getLeaderId(), req.getTerm());
            }

            int idx = req.getPrevLogIndex();
            for (byte[] entry : req.getEntries()) {
                log.write(req.getTerm(), ++idx, entry);
            }

            // Apply all up to what the leader has committed.
            for (int commitIndex = log.lastApplied()+1; commitIndex <= req.getLeaderCommit(); ++commitIndex) {
                log.apply(commitIndex);
            }

            receiveTimer.set(followerTimeoutMs);

            // Always fetch this from the log to ensure the leader didn't send us optimistic (but wrong) values.
            final int nextCommitIndex = log.last().getIndex() + 1;

            LOG.info("Accepted log update from {}.", req.getLeaderId());
            resp = req.accept(io.getNodeId(), nextCommitIndex);
        }
        else {
            LOG.info("Rejecting from {}. We don't have entry term/index {}/{}.", req.getLeaderId(), req.getPrevLogTerm(), req.getPrevLogIndex());
            resp = req.reject(io.getNodeId(), log.last().getIndex()+1);
        }

        // Send response back to leader.
        return resp;
    }

    /**
     * Initialization to be done upon becoming a leader.
     *
     * @throws JiraffetIOException On any error.
     */
    public void leaderInit() throws JiraffetIOException {
        // A new leader first initializes stuff.
        for (final String key : io.nodes()) {
            nextIndex.put(key, log.last().getIndex()+1);
        }
    }

    public void shutdown() {
        running = false;
    }

    /**
     * Run the event loop.
     *
     * @throws JiraffetIOException on any IO error.
     */
    public void run() throws JiraffetIOException
    {
        running = true;
        mode = State.FOLLOWER;
        currentLeader = log.getVotedFor();

        // How long should we wait for messages before we consider things timed out.
        receiveTimer = new Timer(followerTimeoutMs);

        while (running) {

            // Two lists for handling client requests.
            // If a message makes it into here, we think we are the leader and will try to handle it.
            final List<ClientRequest> clientRequests = new ArrayList<>();

            try {

                LOG.debug("Waiting at most {} ms for messages.", receiveTimer.remainingNoThrow());

                LOG.debug("Done waiting. Timer now {} ms.", receiveTimer.remainingNoThrow());

                // Note: We do not check our timer but rely on it to throw a TimeoutException at which point
                //       heartbeats are sent.
                if (clientRequests.size() > 0) {
                    handleClientRequests(clientRequests);
                }
            }
            catch (final JiraffetIOException e) {
                LOG.error("Event loop", e);
            }
            /*
            catch (final InterruptedException e) {
                LOG.error("Interrupted exception in event loop", e);
            }
            catch (final TimeoutException e) {
                switch (mode) {
                case LEADER:
                    LOG.debug("Timeout. Sending heartbeats.");
                    handleClientRequests(clientRequests);
                    break;
                case FOLLOWER:
                case CANDIDATE:
                    LOG.debug("Timeout. Starting an election.");
                    startElection();
                    break;
                }
            }
            */
        }
    }

    /**
     * Just send a heartbeat.
     */
    public void heartBeat() throws JiraffetIOException {
        handleClientRequests(new ArrayList<>(0));
    }

     public void handleClientRequests(final List<ClientRequest> clientRequests) throws JiraffetIOException {
        switch (mode){
        case LEADER:
            appendClientRequests(clientRequests);
            break;
        case FOLLOWER:
        case CANDIDATE:
            for (final ClientRequest req : clientRequests) {
                req.complete(false, currentLeader, "Not leader.");
            }
            break;
        }

    }

    /**
     * If we are a leader, commit these logs to other nodes and then commit them locally and apply them.
     *
     * @param clientRequests Requests from the client on this machine.
     * @throws JiraffetIOException On any error.
     */
    private void appendClientRequests(final List<ClientRequest> clientRequests) throws JiraffetIOException {

        // Set the timer first to avoid a timeout.
        receiveTimer.set(leaderTimeoutMs);

        // For every node we know about, build a request list.
        final List<String> nodes = io.nodes();
        final List<AppendEntriesRequest> requests = new ArrayList<>(nodes.size());

        final int lastIndex = log.last().getIndex();

        for (final String node : nodes) {
            final int nextExpectedIndex = nextIndex.get(node);
            final EntryMeta previousMeta = log.getMeta(nextExpectedIndex-1);
            final List<byte[]> entries = new ArrayList<>();

            // If we don't think the follower is caught up.
            if (nextExpectedIndex < lastIndex) {
                // If the follower is not caught up, attempt to catch them up with this message.
                for (int i = nextExpectedIndex; i <= lastIndex && entries.size() < LOG_ENTRY_LIMIT; ++i) {
                    entries.add(log.read(i));
                }
            }

            // If the client can be caught up with the current list of entries, include the client requests.
            if (previousMeta.getIndex() + entries.size() == lastIndex) {
                for (final ClientRequest cr : clientRequests) {
                    entries.add(cr.getData());
                }
            }

            // Add the final request to our list of messages to send.
            requests.add(
                new AppendEntriesRequest(
                    log.getCurrentTerm(),
                    currentLeader,
                    previousMeta.getIndex(),
                    previousMeta.getTerm(),
                    entries,
                    lastIndex
                )
            );
        }

        // Do the IO.
        final List<AppendEntriesResponse> responses = io.appendEntries(nodes, requests);

        // Collect all the known next indexes and votes.
        int votes = 0;
        for (final AppendEntriesResponse response: responses) {
            nextIndex.put(response.getFrom(), response.getNextCommitIndex());

            // If the client expects data after what this will be when we write to our log, vote.
            if (lastIndex + clientRequests.size() < response.getNextCommitIndex()) {
                votes++;
            }
        }

        int index = lastIndex;
        int currentTerm = log.getCurrentTerm();

        // The followers all have a copy. Lets persist what we have and tell the user.
        for (final ClientRequest cr: clientRequests) {
            index++;
            log.write(currentTerm, index, cr.getData());
        }


        // Finally, tally the votes.
        if (votes + 1 > (io.nodeCount()+1)/2) {

            // Apply everything we replicated.
            for (int i = log.lastApplied()+1; i < index; ++i) {
                log.apply(index);
            }

            // Tell the user the good news.
            for (ClientRequest cr: clientRequests) {
                cr.complete(true, currentLeader, "");
            }
        }
        else {
            for (ClientRequest cr: clientRequests) {
                cr.complete(false, currentLeader, "replication");
            }
        }
    }

    /**
     * Send a {@link RequestVoteRequest} to all. We have not heard anything from a leader in some timeout.
     */
    public void startElection() {
        try {
            if (mode != State.CANDIDATE) {
                mode = State.CANDIDATE;
            }
            
            // Always increment the current term.
            log.setCurrentTerm(log.getCurrentTerm() + 1);

            // votes = 1, we vote for ourselves.
            votes = 1;
            log.setVotedFor(io.getNodeId());

            // There is no current leader.
            currentLeader = null;
            
            // If we are the only node we are the leader by special base-case logic.
            if (io.nodeCount() == 0) {
                currentLeader = io.getNodeId();
                mode = State.LEADER;
                log.setVotedFor(null);
            }
            else {
                final List<RequestVoteResponse> voteResps = io.requestVotes(new RequestVoteRequest(log.getCurrentTerm(), io.getNodeId(), log.last()));
                for (final RequestVoteResponse voteResp : voteResps) {
                    if (voteResp.isVoteGranted()) {
                        votes++;
                    }
                }

                if (votes > (io.nodeCount() + 1)/ 2) {
                    LOG.info("Node {} believes itself the leader for term {}.", io.getNodeId(), log.getCurrentTerm());
                    currentLeader = io.getNodeId();
                    mode = State.LEADER;
                    log.setVotedFor(null);

                    // Heartbeat.
                    appendClientRequests(new ArrayList<>(0));

                    // Use the shorter leader timeout to heartbeat.
                    receiveTimer.set(leaderTimeoutMs);
                }
            }
        }
        catch (final JiraffetIOException e) {
            LOG.error("Starting election.", e);
        }
    }

    /**
     * Send a {@link RequestVoteResponse} in respose to req.
     *
     * @param req A request for votes received from our IO layer.
     */
    public RequestVoteResponse requestVotes(final RequestVoteRequest req) throws JiraffetIOException {
        try {
            final String candidateId = req.getCandidateId();
            
            // If it's an old term, reject.
            if (req.getTerm() <= log.getCurrentTerm()) {
                LOG.debug("Rejecting vote request from {} term {}.", candidateId, req.getTerm());
                return req.reject();
            }
            
            final EntryMeta lastLog = log.last();
            
            // If the candidate asking for our vote has logs in the future, we will vote for them.
            if (log.getVotedFor() == null || req.getLastLogTerm() >= lastLog.getTerm() && req.getLastLogIndex() >= lastLog.getIndex()) {
                LOG.debug("Voting for {} term {}.", candidateId, req.getTerm());
                // If we get here, well, vote!
                log.setVotedFor(candidateId);

                // If we vote for them, reset the timeout and continue.
                receiveTimer.set(followerTimeoutMs);

                return req.vote();
            }
            else {
                LOG.debug("Rejecting vote request from {} term {} log term {} log idx {}.", new Object[]{candidateId, req.getTerm(), req.getLastLogTerm(), req.getLastLogIndex()});
                // If the potential leader does not have at LEAST our last log entry, reject.
                return req.reject();
            }
        }
        catch (final JiraffetIOException e) {
            LOG.error("Casting vote.", e);
            throw e;
        }
    }

    /**
     * Return the nodeId of the node that we believe is the current leader.
     *
     * @return the nodeId of the node that we believe is the current leader.
     */
    public String getCurrentLeader() {
        return currentLeader;
    }

    public boolean isLeader() {
        return getCurrentLeader().equals(io.getNodeId());
    }
}
