package com.github.basking2.jiraffet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.ClientRequest;
import com.github.basking2.jiraffet.messages.Message;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;
import com.github.basking2.jiraffet.util.Timer;
import com.github.basking2.jiraffet.util.VersionVoter;

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

    final VersionVoter versionVoter = new VersionVoter(io.nodeCount());

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
    private long electionTimeoutMs;

    /**
     * Timeout after which a leader should send a heartbeat to prevent an election.
     */
    private long leaderTimeoutMs;

    public Jiraffet(final LogDao log, final JiraffetIO io) {
        this.io = io;
        this.log = log;
        this.mode = State.FOLLOWER;
        this.id = UUID.randomUUID().toString();
        this.commitIndex = 0;
        this.lastApplied = 0;
        this.nextIndex = new HashMap<>();
        this.running = false;
    }


    /**
     * Invoked by leader to replicate log entries; also used as heartbeat.
     *
     * This creates a request that should be sent to followers.
     *
     * @param id The ID of the node to send messages to.
     * @return Request to send to followers.
     * @throws IOException Any IO error.
     */
    public AppendEntriesRequest appendEntries(final String id) throws IOException {

        if (!nextIndex.containsKey(id)) {
            nextIndex.put(id, commitIndex);
        }

        final int index = nextIndex.get(id);


        final List<byte[]> entries = new ArrayList<>(commitIndex - index);

        for (int i = index; i < commitIndex; ++i) {
            entries.add(log.read(i));
        }

        return new AppendEntriesRequest(log.getCurrentTerm(), id, log.getMeta(index-1), entries, commitIndex);
    }

    /**
     * Invoked by leader to replicate log entries; also used as heartbeat.
     *
     * This is executed on the follower when an AppendEntriesRequest is received.
     *
     * @param req The request.
     * @return A response to be sent to the leader.
     * @throws IOException Any IO error.
     */
    public AppendEntriesResponse appendEntries(final AppendEntriesRequest req) throws IOException{

        // If the term is different than our current one adjust our term and make sure we are a follower.
        if (req.getTerm() > log.getCurrentTerm()) {
            log.setCurrentTerm(req.getTerm());
            mode = State.FOLLOWER;
        }

        // There is another leader do not commit this log.
        if (req.getTerm() < log.getCurrentTerm()) {
            return req.reject(id, log.last().getIndex());
        }

        // If the leader knows the previous log state, we can apply this.
        if (log.hasEntry(req.getPrevLogIndex(), req.getPrevLogTerm())) {
            currentLeader = req.getLeaderId();

            int idx = req.getPrevLogIndex();
            for (byte[] entry : req.getEntries()) {
                log.write(req.getTerm(), ++idx, entry);
            }

            if (req.getLeaderCommit() > commitIndex) {
                commitIndex = Math.min(req.getLeaderCommit(), idx);
            }

            applyCommitted();

            return req.accept(id);
        }
        else {
            return req.reject(id, log.last().getIndex());
        }

    }

    public void applyCommitted() {
        // We just got new stuff. Try to update our appended progress.
        while (commitIndex > lastApplied) {
            log.apply(++lastApplied);
        }
    }

    /**
     * Initialization to be done upon becoming a leader.
     * @throws IOException On any error.
     */
    public void leaderInit() throws IOException {
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
     * @throws IOException on any IO error.
     */
    public void run() throws IOException
    {
        running = true;
        mode = State.FOLLOWER;
        currentLeader = log.getVotedFor();

        // How long should we wait for messages before we consider things timed out.
        final Timer timer = new Timer(electionTimeoutMs);

        while (running) {

            // Two lists for handling client requests.
            // If a message makes it into here, we think we are the leader and will try to handle it.
            final List<ClientRequest> clientRequests = new ArrayList<>();

            try {

                // Wait for messages or timeout.
                final List<Message> messages = io.getMessages(timer.remaining(), TimeUnit.MILLISECONDS);

                // If we get no messages, listen again until our timeout is reached.
                if (messages.size() == 0) {
                    throw new TimeoutException("No messages returned.");
                }

                // Process all the messages we received.
                for (final Message m : messages) {

                    // Collect client requests. We must be a leader for this to make sense.
                    if (m instanceof ClientRequest) {
                        final ClientRequest req = (ClientRequest) m;

                        // Batch up messages to send, assuming nothing disrupts us.
                        clientRequests.add(req);

                        continue;
                    }


                    // The leader is asking us to write data.
                    if (m instanceof AppendEntriesRequest) {
                        final AppendEntriesRequest req = (AppendEntriesRequest) m;

                        // If the leader and term match, try to do this. Reset the timeout to the full value.
                        if (currentLeader.equals(req.getLeaderId()) && log.getCurrentTerm() == req.getTerm()) {
                            appendEntries(req);
                            timer.set(electionTimeoutMs);
                        }

                        continue;
                    }


                    // Someone wants to be have an election.
                    if (m instanceof RequestVoteRequest) {
                        final RequestVoteRequest req = (RequestVoteRequest) m;

                        // Handle vote requests.
                        requestVotes(req);

                        // If we vote for them, reset the timeout and continue.
                        timer.set(electionTimeoutMs);

                        continue;
                    }

                    // We are a candidate and get a response.
                    if (m instanceof RequestVoteResponse) {
                        final RequestVoteResponse req = (RequestVoteResponse) m;
                        if (mode == State.CANDIDATE) {

                            if (req.isVoteGranted()) {
                                votes++;
                            }

                            if (votes > io.nodeCount() / 2) {
                                // Heartbeat.
                                appendEntries(new ArrayList<>(0));

                                // Use the shorter leader timeout to heartbeat.
                                timer.set(leaderTimeoutMs);
                            }
                        }

                        continue;
                    }

                    if (m instanceof AppendEntriesResponse) {
                        final AppendEntriesResponse req = (AppendEntriesResponse) m;

                        switch (mode) {
                        case LEADER:

                            // Update the next index.
                            nextIndex.put(req.getFrom(), req.getNextCommitIndex());

                            // Update the vote totals which may trigger other updates.
                            versionVoter.vote(req.getNextCommitIndex()-1);

                            timer.set(leaderTimeoutMs);
                            break;
                        default:
                        }

                        // This is a response from a slow client. Do nothing.
                        continue;
                    }
                }

                // After the main message event handling section, send out all messages if our leadership hasn't changed.
                handleClientRequests(clientRequests);
            }
            catch (final IOException e) {
                LOG.error("Event loop", e);
            }
            catch (final TimeoutException e) {
                switch (mode) {
                case LEADER:
                    appendEntries(clientRequests);
                    timer.set(leaderTimeoutMs);
                    break;
                case FOLLOWER:
                case CANDIDATE:
                    timer.set(leaderTimeoutMs);
                    startElection();
                    break;
                }
            }
        }
    }

    private void handleClientRequests(final List<ClientRequest> clientRequests) throws IOException {
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

    private void appendEntries(final List<ClientRequest> clientRequests) throws IOException {

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
            for (ClientRequest cr : clientRequests) {
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
     * Invoked by candidates to gether votes.
     *
     * This is the candidate side of the function.
     *
     * @return A request to send to all cluster members.
     * @throws IOException on any error.
     */
    public RequestVoteRequest requestVote() throws IOException
    {
        // Vote for yourself.
        log.setCurrentTerm(log.getCurrentTerm()+1);
        log.setVotedFor(id);

        // Build and return a request to send to all other system.
        return new RequestVoteRequest(log.getCurrentTerm(), id, log.last());
    }

    /**
     * Invoked by candidates to gather votes.
     *
     * This is the follower side of the function.
     *
     * @param req The request received from a candidate.
     * @return Return the result that should be sent back to the client.
     * @throws IOException on any error.
     */
    public RequestVoteResponse requestVote(final RequestVoteRequest req) throws IOException
    {
        if (req.getTerm() < log.getCurrentTerm()) {
            return req.reject();
        }

        // If we've voted for someone else.
        if (log.getVotedFor() != null && !log.getVotedFor().equalsIgnoreCase(req.getCandidateId())) {
            return req.reject();
        }

        final LogDao.EntryMeta lastLog = log.last();
        if (req.getLastLogIndex() < lastLog.getIndex() || req.getLastLogTerm() < lastLog.getTerm()) {
            return req.reject();
        }

        return req.vote();

    }

    public void startElection() {
        try {
            mode = State.CANDIDATE;
            // votes = 1, we vote for ourselves.
            votes = 1;
            currentLeader = null;
            versionVoter.clear();
            log.setVotedFor(id);
            log.setCurrentTerm(log.getCurrentTerm()+1);
            io.requestVotes(requestVote());
        }
        catch (final IOException e) {
            LOG.error("Staring election.", e);
        }
    }

    public void requestVotes(final RequestVoteRequest req) {
        try {
            // If it's the wrong term, reject.
            if (req.getTerm() <= log.getCurrentTerm()) {
                io.requestVotes(req.reject());
                return;
            }

            // We don't know this log entry is persisted, so don't vote.
            if (!log.hasEntry(req.getLastLogIndex(), req.getLastLogTerm())) {
                io.requestVotes(req.reject());
                return;
            }

            // If we get here, well, vote!
            io.requestVotes(req.vote());
            log.setVotedFor(req.getCandidateId());
        }
        catch (final IOException e) {
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
