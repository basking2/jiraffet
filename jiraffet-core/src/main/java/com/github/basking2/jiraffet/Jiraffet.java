package com.github.basking2.jiraffet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.LogDao.EntryMeta;
import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.ClientRequest;
import com.github.basking2.jiraffet.messages.Message;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;
import com.github.basking2.jiraffetdb.util.Timer;
import com.github.basking2.jiraffetdb.util.VersionVoter;

/**
 * An instance of the Raft algorithm.
 */
public class Jiraffet
{
    public static final Logger LOG = LoggerFactory.getLogger(Jiraffet.class);

    /**
     * This node's ID.
     */
    private String id;

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
     * @param id How we are identified on the network. This must be sufficient for other nodes to connect to us
     *           as it will be advertised and used when we vote for ourselves.
     * @param log Where entries are committed and applied. Also it holds some persistent state such
     *            as the current term and whom we last voted for.
     * @param io How messsages are sent to other nodes.
     */
    public Jiraffet(final String id, final LogDao log, final JiraffetIO io) {
        this.io = io;
        this.log = log;
        this.mode = State.FOLLOWER;
        this.id = id;
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.leaderTimeoutMs = 5000;
        this.followerTimeoutMs = 2 * this.leaderTimeoutMs;
        this.nextIndex = new HashMap<>();
        this.running = false;
        this.receiveTimer = new Timer(followerTimeoutMs);
        this.versionVoter = new VersionVoter(io.nodeCount());
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

        if (!nextIndex.containsKey(id)) {
            nextIndex.put(id, commitIndex);
        }

        final int index = nextIndex.get(id);

        final List<byte[]> entries;

        // If we have fewer commitIndex values than another node... that is strange. Send an empty log.
        if (commitIndex < index) {
            entries = new ArrayList<>();
        }
        else {

            entries = new ArrayList<>(commitIndex - index);

            for (int i = index; i < commitIndex; ++i) {
                entries.add(log.read(i));
            }
        }

        return new AppendEntriesRequest(log.getCurrentTerm(), currentLeader, log.getMeta(index-1), entries, commitIndex);
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
            resp = req.reject(id, log.last().getIndex());
        }

        // If the leader knows the previous log state, we can apply this.
        else if (log.hasEntry(req.getPrevLogIndex(), req.getPrevLogTerm())) {

            // If the term is greater, we have a new leader. Adjust things.
            if (req.getTerm() > log.getCurrentTerm()) {
                LOG.info("New leader {}.", req.getLeaderId());
                log.setVotedFor(null);
                log.setCurrentTerm(req.getTerm());
                mode = State.FOLLOWER;
                currentLeader = req.getLeaderId();
                receiveTimer.set(followerTimeoutMs);
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

            LOG.info("Accepted log update from {}.", req.getLeaderId());
            resp = req.accept(id);
        }
        else {
            LOG.info("Rejecting from {}. We don't have entry term/index {}/{}.", req.getLeaderId(), req.getPrevLogTerm(), req.getPrevLogIndex());
            resp = req.reject(id, log.last().getIndex());
        }

        // Send response back to leader.
        io.appendEntries(req.getLeaderId(), resp);
    }

    public void applyCommitted() {
        // We just got new stuff. Try to update our appended progress.
        while (commitIndex > lastApplied) {
            log.apply(++lastApplied);
        }
    }

    /**
     * Initialization to be done upon becoming a leader.
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
                                LOG.info("Node {} believes itself the leader for term {}.", id, log.getCurrentTerm());
                                currentLeader = id;
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

    private void appendEntries(final List<ClientRequest> clientRequests) throws JiraffetIOException {

        // Set the timer first to avoid a timeout.
        receiveTimer.set(leaderTimeoutMs);

        // Commit to our local store. Don't tell the client we're done yet, though.
        // Do not increment commitIndex until the majority of followers acknowledge a write.
        for (final ClientRequest clientRequest: clientRequests) {
            log.write(log.getCurrentTerm(), commitIndex+1, clientRequest.getData());
        }

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
            log.setVotedFor(id);
            versionVoter.clear();

            // There is no current leader.
            currentLeader = null;

            io.requestVotes(new RequestVoteRequest(log.getCurrentTerm(), id, log.last()));

            receiveTimer.set((long)(Math.random()*leaderTimeoutMs));
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
            if (req.getLastLogTerm() >= lastLog.getTerm() && req.getLastLogIndex() >= lastLog.getIndex()) {
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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public LogDao getLog() {
        return log;
    }

    public void setLog(LogDao log) {
        this.log = log;
    }

    public JiraffetIO getIo() {
        return io;
    }

    public void setIo(JiraffetIO io) {
        this.versionVoter.clear();
        this.versionVoter.setVoters(io.nodeCount());
        this.io = io;
    }
}
