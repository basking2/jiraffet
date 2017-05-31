package com.github.basking2.jiraffet;

import java.util.List;
import java.util.concurrent.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.ClientRequest;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;

/**
 * This class runs Jiraffet. It uses a {@link ScheduledExecutorService} to time heartbeats and elections.
 *
 * This class will synchronized on the instance of {@link JiraffetRaft} passed to it.
 * As such, this class is thread-safe.
 */
public class Jiraffet {

    private static final Logger LOG = LoggerFactory.getLogger(Jiraffet.class);

    private final JiraffetRaft raft;

    private long leaderTimeoutMs;
    private long followerTimeoutMs;
    private boolean running;
    private ScheduledExecutorService scheduledExecutorService;

    /**
     * Periodically wake up and send heartbeats of things have been quiet.
     */
    private Callable<Void> leaderHeartbeats;

    /**
     * Periodically wake up and become the leader if we haven't heared from a leader in a while.
     */
    private Callable<Void> followerElections;

    /**
     * The epoch ({@link System#currentTimeMillis()}) since something last happened that would reset a timer.
     */
    private volatile long lastActivity;

    /**
     * A construtor that uses {@link Executors#newSingleThreadScheduledExecutor()}.
     *
     * @param raft The logic to handle raft messages.
     * @see #Jiraffet(JiraffetRaft, ScheduledExecutorService)
     */
    public Jiraffet(
            final JiraffetRaft raft
    ) {
        this(raft, Executors.newSingleThreadScheduledExecutor());
    }

    /**
     * Constructor.
     *
     * @param raft The logic to handle raft messages.
     * @param scheduledExecutorService Handle timers for elections and heartbeats.
     */
    public Jiraffet(
            final JiraffetRaft raft,
            final ScheduledExecutorService scheduledExecutorService
    ) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.raft = raft;
        this.leaderTimeoutMs = 5000L;
        this.followerTimeoutMs = 4 * this.leaderTimeoutMs;
        this.leaderHeartbeats = buildLeaderHeartbeats();
        this.followerElections = buildFollowerElections();
    }

    private Callable<Void> buildLeaderHeartbeats() {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                LOG.info("Leader heartbeat.");
                if (!running) {
                    throw new Exception("Not running!");
                }

                try {
                    synchronized (raft) {

                        if (!raft.isLeader()) {
                            scheduleAsFollower();
                            throw new Exception("We are not the leader!");
                        }

                        LOG.info("No activity for {} ms.", System.currentTimeMillis() - lastActivity);

                        // If the leader timeout has expired, send heartbeats.
                        if (System.currentTimeMillis() - lastActivity >= leaderTimeoutMs) {
                            heartbeats();
                        }

                        final long sinceLastActivity = System.currentTimeMillis() - lastActivity;
                        scheduledExecutorService.schedule(this, leaderTimeoutMs - sinceLastActivity, TimeUnit.MILLISECONDS);
                    }
                }
                catch (final Throwable t) {
                    LOG.error("Follower Election Crash.", t);
                }

                return null;
            }
        };
    }

    private Callable<Void> buildFollowerElections() {
        return new Callable<Void>(){
            @Override
            public Void call() throws Exception {
                LOG.info("Follower election check.");
                if (!running) {
                    LOG.info("Not running! Follower check exiting.");
                    throw new Exception("Not running!");
                }

                try {
                    synchronized (raft) {
                        // If we are the leader, abort.
                        if (raft.isLeader()) {
                            scheduleAsLeader();
                            throw new Exception("We are not a follower!");
                        }

                        final long sinceLastActivity = System.currentTimeMillis() - lastActivity;

                        LOG.info("Last activity {} ms ago.", sinceLastActivity);

                        // No leader has talked to us in quite a while. Let's try to become the leader!
                        if (sinceLastActivity >= followerTimeoutMs) {
                            try {
                                raft.startElection();
                            } catch (final Exception e) {
                                LOG.error("Starting election.", e);
                            }

                            // If we became the leader, schedule that work!
                            if (raft.isLeader()) {
                                scheduleAsLeader();
                            } else {
                                // Retry after a random sleep.
                                long sleep = 0;
                                do {
                                    sleep = (long) (Math.random() * followerTimeoutMs);
                                } while (sleep == 0);

                                scheduledExecutorService.schedule(this, sleep, TimeUnit.MILLISECONDS);
                            }
                        }
                        else {
                            scheduledExecutorService.schedule(this, followerTimeoutMs - sinceLastActivity, TimeUnit.MILLISECONDS);
                        }
                    }
                }
                catch (final Throwable t) {
                    LOG.error("Follower Election Crash.", t);
                }

                return null;
            }
        };
    }
    
    public void start() throws JiraffetIOException {
        raft.start();
        running = true;
        lastActivity = System.currentTimeMillis();
        scheduleAsFollower();
    }

    public void stop() {
        running = false;
    }
    
    private void scheduleAsFollower() {
        LOG.info("Scheduing follower check in {} ms.", followerTimeoutMs);
        scheduledExecutorService.schedule(followerElections, followerTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void scheduleAsLeader() throws JiraffetIOException {
        raft.leaderInit();

        LOG.info("Scheduling leader heartbeats.");

        heartbeats();

        scheduledExecutorService.schedule(leaderHeartbeats, leaderTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * If another node is asking for our vote.
     *
     * @param request The request.
     * @return The response.
     * @throws JiraffetIOException On errors.
     */
    public RequestVoteResponse requestVotes(final RequestVoteRequest request) throws JiraffetIOException {
        synchronized (raft) {
            final RequestVoteResponse r = raft.requestVotes(request);
            return r;
        }
    }

    /**
     * If another node, a leader, asks us to append entries.
     * @param request The requested entries to add.
     * @return The response.
     * @throws JiraffetIOException On any errors.
     */
    public AppendEntriesResponse appendEntries(final AppendEntriesRequest request) throws JiraffetIOException {
        synchronized (raft) {

            final long time = System.currentTimeMillis();

            final AppendEntriesResponse r = raft.appendEntries(request);

            if (request.getLeaderId().equalsIgnoreCase(raft.getCurrentLeader())) {
                lastActivity = time;
            }

            return r;
        }
    }

    /**
     * Submit new data.
     *
     * @param requests The client's requests.
     *
     * @throws JiraffetIOException on errors.
     */
    public void append(final List<ClientRequest> requests) throws JiraffetIOException {
        synchronized (raft) {
            // Record when we start work.
            final long time = System.currentTimeMillis();

            // Leader or not, let JiraffetRaft produce the response.
            raft.handleClientRequests(requests);

            // Only update the activity if we are the leader.
            if (raft.isLeader()) {
                lastActivity = time;
            }
        }
    }

    /**
     * Send heartbeats. Only call if we are the leader.
     */
    public void heartbeats() {
        synchronized (raft) {
            try {
                final long time = System.currentTimeMillis();

                raft.heartBeat();

                // Only record this activity if we are the leader.
                if (raft.isLeader()) {
                    lastActivity = time;
                }
            }
            catch (final Exception e) {
                LOG.error("Sending heartbeats.", e);
            }
        }
    }

    final public String getCurrentLeader() {
        return raft.getCurrentLeader();
    }

    final public String getNodeId() {
        return raft.getNodeId();
    }
}
