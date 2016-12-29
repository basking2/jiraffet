package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.jiraffet.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
    private Timer receiveTimer;
    private boolean running;

    public JiraffetAccess(final Jiraffet jiraffet, final JiraffetIO io, final LogDao log, final Timer followerTimer, final Timer leaderTimer) {
        this.jiraffet = jiraffet;
        this.io = io;
        this.log = log;

        this.leaderTimeoutMs = 5000L;
        this.followerTimeoutMs = 4 * this.leaderTimeoutMs;
        this.receiveTimer = new Timer(followerTimeoutMs);
    }

    public void run() {
        running = true;

        // FIXME - adhere to timer resets from requestVotes() and appendEntries().
        while (running) {
            receiveTimer.waitRemaining();

            synchronized (jiraffet) {
                if (jiraffet.isLeader()) {
                    try {
                        jiraffet.heartBeat();
                    } catch (final JiraffetIOException e) {
                        LOG.error(e.getMessage(), e);
                    }
                } else {
                    jiraffet.startElection();
                }
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
        // FIXME - reset timer.
        synchronized (jiraffet) {
            return jiraffet.requestVotes(request);
        }
    }

    public AppendEntriesResponse appendEntries(final AppendEntriesRequest request) throws JiraffetIOException {
        // FIXME - reset timer.
        synchronized (jiraffet) {
            return jiraffet.appendEntries(request);
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
