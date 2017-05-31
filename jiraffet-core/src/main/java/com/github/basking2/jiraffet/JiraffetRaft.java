package com.github.basking2.jiraffet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.github.basking2.jiraffet.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.JiraffetLog.EntryMeta;

/**
 * An instance of the Raft algorithm.
 *
 * This class cannot be used concurrently and does not offer a mechanism to handle timers.
 *
 * See the {@link Jiraffet} class for a way to use this in an application
 */
public class JiraffetRaft
{
    public static final Logger LOG = LoggerFactory.getLogger(JiraffetRaft.class);

    /**
     * A limit on the number of log messages sent in any update.
     */
    private static final int LOG_ENTRY_LIMIT = 10;

    private String currentLeader;

    /**
     * The log the Raft algorithm is managing..
     */
    private JiraffetLog log;

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

    enum State {
        CANDIDATE,
        FOLLOWER,
        LEADER
    };

    /**
     * @param log Where entries are committed and applied. Also it holds some persistent state such
     *            as the current term and whom we last voted for.
     * @param io How messages are sent to other nodes.
     */
    public JiraffetRaft(final JiraffetLog log, final JiraffetIO io) {
        this.io = io;
        this.log = log;
        this.mode = State.FOLLOWER;
        this.nextIndex = new HashMap<>();
        this.currentLeader = "";
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

        final List<LogEntry> entries;

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
                entries.add(log.getLogEntry(nextExpectedIndex+i));
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
     *
     * @param leaderId The new leader's ID.
     * @param term The term this leader is presiding over.
     * @throws JiraffetIOException Any error.
     */
    public void setNewLeader(final String leaderId, int term) throws JiraffetIOException {
        LOG.info("New leader {}.", leaderId);
        log.setVotedFor(null);
        log.setCurrentTerm(term);
        mode = State.FOLLOWER;
        currentLeader = leaderId;
    }

    /**
     * Invoked by leader to replicate log entries; also used as heartbeat.
     *
     * This is executed on the follower when an AppendEntriesRequest is received.
     *
     * @param req The request.
     * @return The response.
     * @throws JiraffetIOException Any IO error.
     */
    public AppendEntriesResponse appendEntries(final AppendEntriesRequest req) throws JiraffetIOException {

        final AppendEntriesResponse resp;

        // If the leader and term match, try to do this.
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

            for (LogEntry entry : req.getEntries()) {
                log.write(entry.getTerm(), entry.getIndex(), entry.getData());
            }

            // Pick the leader's commit index or the last entry in our log, which ever is smaller.
            final int commitLimit = Math.min(req.getLeaderCommit(), log.last().getIndex());

            // Apply all up to what the leader has committed up to the maximum we have in our log.
            for (int commitIndex = log.lastApplied()+1; commitIndex <= commitLimit; ++commitIndex) {
                log.apply(commitIndex);
            }

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

    /**
     * Run the event loop.
     *
     * @throws JiraffetIOException on any IO error.
     */
    public void start() throws JiraffetIOException
    {
        mode = State.FOLLOWER;
        currentLeader = log.getVotedFor();
    }

    /**
     * Just send a heartbeat.
     * @throws JiraffetIOException on errors.
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

        // For every node we know about, build a request list.
        final List<String> nodes = io.nodes();
        final List<AppendEntriesRequest> requests = new ArrayList<>(nodes.size());

        final int lastIndex = log.last().getIndex();

        for (final String node : nodes) {

            if (!nextIndex.containsKey(node)) {
                nextIndex.put(node, log.last().getIndex() + 1);
            }

            final int nextExpectedIndex = nextIndex.get(node);

            final EntryMeta previousMeta = log.getMeta(nextExpectedIndex-1);
            final List<LogEntry> entries = new ArrayList<>();

            // If we don't think the follower is caught up.
            if (nextExpectedIndex <= lastIndex) {
                // If the follower is not caught up, attempt to catch them up with this message.
                for (int i = nextExpectedIndex; i <= lastIndex && entries.size() < LOG_ENTRY_LIMIT; ++i) {
                    entries.add(log.getLogEntry(i));
                }
            }

            // If the client can be caught up with the current list of entries, include the client requests.
            if (previousMeta.getIndex() + entries.size() == lastIndex) {
                int index = log.last().getIndex();
                for (final ClientRequest cr : clientRequests) {
                    final LogEntry logEntry = new LogEntry(++index, log.getCurrentTerm(), cr.getData());
                    entries.add(logEntry);
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

            // If the request is a success and then follower expects data more than our log will eventually hold, vote!
            if (response.isSuccess() && lastIndex + clientRequests.size() < response.getNextCommitIndex()) {
                votes++;
            }
        }

        // If enough clients replicated the log, we will write it, apply it, and
        // on the next follower communication, notify them to apply the log.
        if (votes + 1 > (io.nodeCount()+1)/2) {
            int index = lastIndex;
            final int currentTerm = log.getCurrentTerm();

            // The followers all have a copy. Lets persist what we have and tell the user.
            for (final ClientRequest cr: clientRequests) {
                index++;
                log.write(currentTerm, index, cr.getData());
            }

            // Apply everything we replicated.
            for (int i = log.lastApplied()+1; i <= index; ++i) {
                log.apply(i);
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
     * @return The vote response.
     * @throws JiraffetIOException on errors.
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
        final String currentLeader = getCurrentLeader();

        if (currentLeader == null) {
            return false;
        }

        return currentLeader.equals(io.getNodeId());
    }

    public String getNodeId() {
        return io.getNodeId();
    }
}
