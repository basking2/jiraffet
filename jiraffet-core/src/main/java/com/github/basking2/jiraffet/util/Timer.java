package com.github.basking2.jiraffet.util;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Track time progress, giving the user the time remaining for a timer.
 */
public class Timer {

    /**
     * Alias the value {@link TimeUnit#MILLISECONDS}.
     *
     * The Timer class expresses things at the resolution of milliseconds.
     */
    public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * The system clock time when this will be expired.
     */
    private long systemTime;

    /**
     * Used to reset this timer.
     */
    private long timeout;

    /**
     * Construct a timeout for the given number of milliseconds.
     *
     * @param timeout Milliseconds plus the system clock after which this Timer is expired.
     */
    public Timer(final long timeout){
        set(timeout);
    }

    /**
     * Construct a timeout for the give time values.
     *
     * The value is converted into milliseconds.
     *
     * @param timeout The timeout value.
     * @param timeunit The timeunit of timeout. This is used to convert timeout to milliseconds.
     */
    public Timer(final long timeout, final TimeUnit timeunit){
        set(timeunit.toMillis(timeout));
    }

    /**
     * Set the timeout to this value plus the current system time.
     *
     * @param timeout The duration in milliseconds for this Timer to track.
     */
    public void set(final long timeout) {
        this.timeout = timeout;
        reset();
    }

    /**
     * An alias to {@link #reset()}. This starts the timer.
     */
    public void set() {
        reset();
    }
    
    public long get() {
        return timeout;
    }

    /**
     * Record the system time at which time timer will  have expired, effectively starting the timer.
     */
    public void reset() {
        systemTime = System.currentTimeMillis() + timeout;
    }

    /**
     * Return The number of milliseconds computed. This value may be negative or 0.
     *
     * @return The number of milliseconds computed. This value may be negative or 0.
     */
    public long remainingNoThrow() {
        return systemTime - System.currentTimeMillis();
    }

    /**
     * Return The number of milliseconds computed. If this would be 0 or negative a {@link TimeoutException} is thrown.
     * @return The number of milliseconds computed. If this would be 0 or negative a {@link TimeoutException} is thrown.
     * @throws TimeoutException When the timer has expired and the return value would have been 0 or negative.
     */
    public long remaining() throws TimeoutException {
        long t = remainingNoThrow();

        if (t <= 0) {
            throw new TimeoutException("Timer expired.");
        }

        return t;
    }

    /**
     * Use {@link Thread#wait(long)} to sleep the remainder of the time left.
     *
     * If an {@link InterruptedException} is thrown the timer continues waiting.
     */
    public void waitRemaining() {
        for (long t = remainingNoThrow(); t > 0; t = remainingNoThrow()) {
            try {
                Thread.currentThread().wait(t);
            }
            catch (final InterruptedException e) {
            }
        }
    }

}
