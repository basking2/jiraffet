package com.github.basking2.otternet.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Utilities for work with futures.
 */
public class Futures {

    /**
     * Get all futures that complete in the given timeout.
     *
     * @param futures The futures.
     * @param results The results.
     * @param timeout The timeout.
     * @param timeunit The timeout unites.
     * @param <T> The type returned in the results list.
     */
    public static <T> void getAll(final Iterable<Future<T>> futures, final List<T> results, long timeout, TimeUnit timeunit)
    {
        final Iterator<Future<T>> itr = futures.iterator();

        // Compute the system time when we will be out of time.
        final long timeoutExpired = System.nanoTime() / 1000 + timeunit.toMillis(timeout);
        long now = System.nanoTime() / 1000;

        // Fetch as long as the timeout is valid.
        while (now < timeoutExpired && itr.hasNext()) {
            try {
                final Future<T> future = itr.next();
                final T t = future.get(timeoutExpired - now, TimeUnit.MILLISECONDS);
                results.add(t);
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
            catch (TimeoutException e) {
                e.printStackTrace();
            }
        }

        // If we are here, if there are any futures left, get them ONLY if they are complete.
        getAllDone(itr, results);
    }

    /**
     * Get all the futures that are currently done.
     * @param futures The futures.
     * @param results The collection the results will be added to.
     * @param <T> The type.
     */
    public static <T> void getAllDone(final Iterator<Future<T>> futures, final Collection<T> results) {
        while (futures.hasNext()) {
            final Future<T> future = futures.next();
            if (future.isDone()) {
                try {
                    final T t = future.get();
                    results.add(t);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Get all the futures that are currently done.
     * @param futures The futures.
     * @param results The collection the results will be added to.
     * @param <T> The type.
     */
    public static <T> void getAllDone(final Iterable<Future<T>> futures, final Collection<T> results) {
        getAllDone(futures.iterator(), results);
    }
}
