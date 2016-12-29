package com.github.basking2.jiraffet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.basking2.jiraffet.messages.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.LogDao.EntryMeta;
import com.github.basking2.jiraffet.util.Timer;
import com.github.basking2.jiraffet.util.VersionVoter;

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
     * The last log committed by this node.
     */
    private int commitIndex;

    /**
     * The last log entry applied to some state machine.
     */
    private int lastApplied;

    private Map<String, Integer> nextIndex;

    private State mode;

    private int votes;

    private JiraffetIO io;

    final VersionVoter versionVoter;

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
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.leaderTimeoutMs = 5000;
        this.followerTimeoutMs = 4 * this.leaderTimeoutMs;
        this.nextIndex = new HashMap<>();
        this.running = false;
        this.receiveTimer = new Timer(followerTimeoutMs);

        // Notice - Our version voter must require +1 nodes so as to include the leader.
        //          A leader always implicitly votes as having comitted a value.
        this.versionVoter = new VersionVoter(() -> io.nodeCount() + 1);
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
            nextIndex.put(id, commitIndex+1);
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
                commitIndex);
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
     * @throws JiraffetIOException Any IO error.
     */
    public void appendEntries(final AppendEntriesRequest req) throws JiraffetIOException {

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

            if (req.getLeaderCommit() > commitIndex) {
                commitIndex = Math.min(req.getLeaderCommit(), idx);
            }

            applyCommitted();

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
        io.appendEntries(req.getLeaderId(), resp);
    }

    /**
     * The leader has just learned that a majority of nodes have committed {@link #commitIndex}.
     *
     * Thus, it is allowable to apply {@link #lastApplied} up to {@link #commitIndex}.
     */
    public void applyCommitted() {
        // We just got new stuff. Try to update our appended progress.
        while (commitIndex > lastApplied) {
            log.apply(++lastApplied);
        }
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

                // Wait for messages or timeout.
                final List<Message> messages = io.getMessages(receiveTimer.remaining(), TimeUnit.MILLISECONDS);
                LOG.debug("Done waiting. Timer now {} ms.", receiveTimer.remainingNoThrow());

                // If we get no messages, listen again until our timeout is reached.
                if (messages.size() == 0) {
                    LOG.warn("Got 0 messages from IO layer. Interpreting this as a timeout.");
                    throw new TimeoutException("No messages returned.");
                }

                // Process all the messages we received.
                for (final Message m : messages) {

                    if (m instanceof ClientRequest) {
                        // Batch up messages to send, assuming nothing disrupts us.
                        clientRequests.add((ClientRequest)m);
                        continue;
                    }

                    if (m instanceof AppendEntriesRequest) {
                        appendEntries((AppendEntriesRequest)m);
                        continue;
                    }

                    if (m instanceof RequestVoteRequest) {
                        // Handle vote requests.
                        requestVotes((RequestVoteRequest)m);
                        continue;
                    }

                    // We are a candidate and get a response.
                    if (m instanceof RequestVoteResponse) {
                        final RequestVoteResponse req = (RequestVoteResponse) m;

                        // If we are a candidate and got a vote.
                        if (mode == State.CANDIDATE && req.isVoteGranted()){

                            votes++;

                            // Should we win the election.
                            if (votes > io.nodeCount() / 2) {
                                LOG.info("Node {} believes itself the leader for term {}.", io.getNodeId(), log.getCurrentTerm());
                                currentLeader = io.getNodeId();
                                mode = State.LEADER;
                                log.setVotedFor(null);

                                // Heartbeat.
                                appendEntries(new ArrayList<>(0));

                                // Use the shorter leader timeout to heartbeat.
                                receiveTimer.set(leaderTimeoutMs);
                            }
                        }

                        continue;
                    }

                    if (m instanceof AppendEntriesResponse) {
                        final AppendEntriesResponse req = (AppendEntriesResponse) m;

                        switch (mode) {
                        case LEADER:

                            LOG.debug("Node {}'s next commit is {}.", req.getFrom(), req.getNextCommitIndex());

                            // Update the next index.
                            nextIndex.put(req.getFrom(), req.getNextCommitIndex());

                            // Update the vote totals which may trigger other updates.
                            versionVoter.vote(req.getNextCommitIndex()-1);

                            break;
                        default:
                            // Nop. We are not the leader. We ignore this.
                        }

                        continue;
                    }

                    // Sanity check.
                    throw new RuntimeException("We got a message we do not know how to handle: "+m);

                } // after for-messages loop.

                // Note: We do not check our timer but rely on it to throw a TimeoutException at which point
                //       heartbeats are sent.
                if (clientRequests.size() > 0) {
                    handleClientRequests(clientRequests);
                }
            }
            catch (final JiraffetIOException e) {
                LOG.error("Event loop", e);
            }
            catch (final InterruptedException e) {
                LOG.error("Interrupted exception in event loop", e);
            }
            catch (final TimeoutException e) {
                switch (mode) {
                case LEADER:
                    LOG.debug("Timeout. Sending heartbeats.");
                    appendEntries(clientRequests);
                    break;
                case FOLLOWER:
                case CANDIDATE:
                    LOG.debug("Timeout. Starting an election.");
                    startElection();
                    break;
                }
            }
        }
    }

    private void handleClientRequests(final List<ClientRequest> clientRequests) throws JiraffetIOException {
        switch (mode){
        case LEADER:
            appendEntries(clientRequests);
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
     * If we are a leader, append these entries to our local log and replicate out.
     *
     * @param clientRequests Requests from the client on this machine.
     * @throws JiraffetIOException On any error.
     */
    private void appendEntries(final List<ClientRequest> clientRequests) throws JiraffetIOException {

        // Set the timer first to avoid a timeout.
        receiveTimer.set(leaderTimeoutMs);

        // Commit to our local store. Don't tell the client we're done yet, though.
        // Do not increment commitIndex until the majority of followers acknowledge a write.
        for (final ClientRequest clientRequest: clientRequests) {
            LOG.info("Writing client request to {}.", commitIndex+1);
            log.write(log.getCurrentTerm(), commitIndex+1, clientRequest.getData());
        }

        // Only listen for commits if we have added new data. Client requests can be zero when heart beating.
        if (clientRequests.size() > 0) {

            // Set what we do when this version is committed.
            // NOTE: It is possible, in a very lagged deployment, for there to be many
            // versions that become current and having many versionVoter listeners would give us
            // more fine-grained progress. We do not assume this is the normal situation.
            //
            // The assumption in this is that the system will be mostly-consistent at all times.
            versionVoter.setListener(commitIndex + clientRequests.size(), (ver, succ) -> {

                String clientMsg = "";
                boolean clientSucc = succ;

                if (succ) {
                    // If this version won an election, we know it is committed and safe to apply.
                    commitIndex = ver;

                    // Apply committed stuff op to commitIndex.
                    applyCommitted();

                    // FIXME - if there was a problem applying the message, change clientSucc and clientMsg.
                }

                // Tell the clients we've finished their request.
                for (final ClientRequest cr : clientRequests) {
                    cr.complete(clientSucc, currentLeader, clientMsg);
                }
            });

            // We have a vote. :)
            versionVoter.vote(commitIndex + clientRequests.size());
        }

        // Send to all nodes in the cluster their update.
        for (final String id : io.nodes()) {
            final AppendEntriesRequest req = appendEntries(id);
            io.appendEntries(id, req);
        }
    }

    /**
     * Invoked by candidates to gather votes.
     *
     * This is the follower side of the function.
     *
     * @param req The request received from a candidate.
     * @return Return the result that should be sent back to the client.
     * @throws JiraffetIOException on any error.
     */
    public RequestVoteResponse requestVote(final RequestVoteRequest req) throws JiraffetIOException
    {
        // Candidate is requesting a vote for a passed term. Reject.
        if (req.getTerm() < log.getCurrentTerm()) {
            return req.reject();
        }

        // If we've voted for someone else.
        if (log.getVotedFor() != null && !log.getVotedFor().equalsIgnoreCase(req.getCandidateId())) {
            return req.reject();
        }

        // Is the candidate's log at least as up-to-date as our log?
        final LogDao.EntryMeta lastLog = log.last();
        if (req.getLastLogIndex() < lastLog.getIndex() || req.getLastLogTerm() < lastLog.getTerm()) {
            return req.reject();
        }

        return req.vote();

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
            versionVoter.clear();

            // There is no current leader.
            currentLeader = null;
            
            // If we are the only node we are the leader by special base-case logic.
            if (io.nodeCount() == 0) {
                currentLeader = io.getNodeId();
                mode = State.LEADER;
                log.setVotedFor(null);
            }
            else {
                io.requestVotes(new RequestVoteRequest(log.getCurrentTerm(), io.getNodeId(), log.last()));
                receiveTimer.set((long)(Math.random()*leaderTimeoutMs));
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
    public void requestVotes(final RequestVoteRequest req) {
        try {
            final String candidateId = req.getCandidateId();
            
            // If it's an old term, reject.
            if (req.getTerm() <= log.getCurrentTerm()) {
                LOG.debug("Rejecting vote request from {} term {}.", req.getCandidateId(), req.getTerm());
                io.requestVotes(candidateId, req.reject());
                return;
            }
            
            final EntryMeta lastLog = log.last();
            
            // If the candidate asking for our vote has logs in the future, we will vote for them.
            if (log.getVotedFor() == null || req.getLastLogTerm() >= lastLog.getTerm() && req.getLastLogIndex() >= lastLog.getIndex()) {
                LOG.debug("Voting for {} term {}.", req.getCandidateId(), req.getTerm());
                // If we get here, well, vote!
                io.requestVotes(candidateId, req.vote());
                log.setVotedFor(req.getCandidateId());

                // If we vote for them, reset the timeout and continue.
                receiveTimer.set(followerTimeoutMs);
            }
            else {
                LOG.debug("Rejecting vote request from {} term {} log term {} log idx {}.", new Object[]{req.getCandidateId(), req.getTerm(), req.getLastLogTerm(), req.getLastLogIndex()});
                // If the potential leader does not have at LEAST our last log entry, reject.
                io.requestVotes(candidateId, req.reject());
            }
        }
        catch (final JiraffetIOException e) {
            LOG.error("Casting vote.", e);
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
