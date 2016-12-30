package com.github.basking2.jiraffet;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

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
    }
    
    public void start() {
        running = true;
        lastActivity = System.currentTimeMillis();
        
        scheduledExecutorService.

        // FIXME - adhere to timer resets from requestVotes() and appendEntries().
        while (running) {
            receiveTimer.waitRemaining();

            synchronized(jiraffet) {
                // If we are the leader, reset.
                if (jiraffet.isLeader()) {
                    try {
                        jiraffet.heartBeat();
                    }
                    catch (final JiraffetIOException e) {
                        LOG.error(e.getMessage(), e);
                    }

                    receiveTimer.reset();
                }
                // If we are the follower and we haven't gotten any heart beats etc...
                else if (System.currentTimeMillis() - lastActivity > receiveTimer.get()) {

                    // FIXME - we need to know if we win the election and how long to sleep if we do.
                    jiraffet.startElection();
                }
            }
        }
    }
    
    private void scheduleAsFollower() {
        // FIXME - handle this, on timeout do an election. 
    }

    private void scheduleAsLeader() {
        // FIXME - handle this, do heart beats.
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
     * @param requests
     *
     * @throws JiraffetIOException
     */
    public void append(final List<ClientRequest> requests) throws JiraffetIOException {
        synchronized (jiraffet) {
            jiraffet.handleClientRequests(requests);
        }
    }
}
