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
 * This class ties together the logic, the storage, and the communication pieces of JiraffetAccess.
 *
 * This class is thread-safe.
 */
public class JiraffetAccess {

    private static final Logger LOG = LoggerFactory.getLogger(JiraffetAccess.class);

    private final JiraffetIO io;
    private final LogDao log;
    private final Jiraffet jiraffet;

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

    public JiraffetAccess(
            final Jiraffet jiraffet,
            final JiraffetIO io,
            final LogDao log
    ) {
        this(jiraffet, io, log, Executors.newSingleThreadScheduledExecutor());
    }

    public JiraffetAccess(
            final Jiraffet jiraffet,
            final JiraffetIO io,
            final LogDao log,
            final ScheduledExecutorService scheduledExecutorService
    ) {
        this.scheduledExecutorService = scheduledExecutorService;
        this.jiraffet = jiraffet;
        this.io = io;
        this.log = log;
        this.leaderTimeoutMs = 5000L;
        this.followerTimeoutMs = 4 * this.leaderTimeoutMs;
        this.leaderHeartbeats = buildLeaderHeartbeats();
        this.followerElections = buildFollowerElections();
    }

    private Callable<Void> buildLeaderHeartbeats() {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                if (!running) {
                    throw new Exception("Not running!");
                }

                synchronized (jiraffet) {

                    if (!jiraffet.isLeader()) {
                        scheduleAsFollower();
                        throw new Exception("We are not the leader!");
                    }

                    // If the leader timeout has expired, send heartbeats.
                    if (System.currentTimeMillis() - lastActivity >= leaderTimeoutMs) {
                        heartbeats();
                    }

                    final long sinceLastActivity = System.currentTimeMillis() - lastActivity;
                    scheduledExecutorService.schedule(this, leaderTimeoutMs - sinceLastActivity, TimeUnit.MILLISECONDS);
                }

                return null;
            }
        };
    }

    private Callable<Void> buildFollowerElections() {
        return new Callable<Void>(){
            @Override
            public Void call() throws Exception {
                if (!running) {
                    throw new Exception("Not running!");
                }

                synchronized (jiraffet) {
                    // If we are the leader, abort.
                    if (jiraffet.isLeader()) {
                        scheduleAsLeader();
                        throw new Exception("We are not a follower!");
                    }

                    // No leader has talked to us in quite a while. Let's try to become the leader!
                    if (System.currentTimeMillis() - lastActivity >= followerTimeoutMs) {
                        try {
                            jiraffet.startElection();
                        }
                        catch (final Exception e) {
                            LOG.error("Starting election.", e);
                        }

                        // If we became the leader, schedule that work!
                        if (jiraffet.isLeader()) {
                            scheduleAsLeader();
                        }
                        else {
                            // Retry after a random sleep.
                            long sleep = 0;
                            do {
                                sleep = (long) (Math.random() * followerTimeoutMs);
                            } while (sleep == 0);

                            scheduledExecutorService.schedule(this, sleep, TimeUnit.MILLISECONDS);
                        }
                    }
                }

                return null;
            }
        };
    }
    
    public void start() {
        running = true;
        lastActivity = System.currentTimeMillis();
        scheduleAsFollower();
    }

    public void stop() {
        running = false;
    }
    
    private void scheduleAsFollower() {
        scheduledExecutorService.schedule(followerElections, followerTimeoutMs, TimeUnit.MILLISECONDS);
    }

    private void scheduleAsLeader() {
        heartbeats();

        scheduledExecutorService.schedule(leaderHeartbeats, leaderTimeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Send heartbeats. Only call if we are the leader.
     */
    public void heartbeats() {
        synchronized (jiraffet) {
            try {
                lastActivity = System.currentTimeMillis();
                jiraffet.heartBeat();
            }
            catch (final Exception e) {
                LOG.error("Sending heartbeats.", e);
            }
        }
    }

    /**
     * If another node is asking for our vote.
     *
     * @param request The request.
     * @return The response.
     * @throws JiraffetIOException On errors.
     */
    public RequestVoteResponse requestVotes(final RequestVoteRequest request) throws JiraffetIOException {
        synchronized (jiraffet) {
            final RequestVoteResponse r = jiraffet.requestVotes(request);
            lastActivity = System.currentTimeMillis();
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
        synchronized (jiraffet) {
            final AppendEntriesResponse r = jiraffet.appendEntries(request);
            lastActivity = System.currentTimeMillis();
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
        synchronized (jiraffet) {
            lastActivity = System.currentTimeMillis();
            jiraffet.handleClientRequests(requests);
        }
    }
}
