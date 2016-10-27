package com.github.basking2.jiraffet.util;

import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class Futures {

    public static <T> void waitAtMost(
            final List<Future<T>> futures,
            final List<T> results,
            final List<Future<T>> unDone,
            long timeout, TimeUnit timeunit
            ) {

        // Get until we hit our timeout.
        final int idx = getUntilTimeout(futures, results, timeout, timeunit);

        // Check and get anything that completed while we waited on earlier futures.
        getReady(futures, idx+1, results, unDone);
    }

    /**
     * Fetch values from futures into results, in order, until a timeout is reached.
     *
     * {@link ExecutionException}, {@link CancellationException} and {@link InterruptedException} are all caught
     * and result in a value being ignored.
     *
     * @param futures
     * @param results
     * @param timeout
     * @param timeunit
     * @param <T>
     * @return The number of elements consumed from futures. This may be less than the length of results
     *         if when calling {@link Future#get()} throws an {@link ExecutionException} or {@link InterruptedException}.
     * @throws TimeoutException If the time expires before all futures are gathered.
     */
    private static <T> int getUntilTimeout(final List<Future<T>> futures, final List<T> results, final long timeout, final TimeUnit timeunit) {

        int idx = 0;
        try {
            for (; idx < futures.size(); idx++) {
                final Timer timer = new Timer(timeout, timeunit);

                try {
                    // Apply our timeout.
                    final T t = futures.get(idx).get(timer.remaining(), TimeUnit.MILLISECONDS);

                    results.add(t);
                } catch (final CancellationException e) {
                    // Nop.
                } catch (final ExecutionException e) {
                    // Nop.
                } catch (final InterruptedException e) {
                    // Nop.
                }
            }
        }
        catch (final TimeoutException e) {
            // Nop.
        }

        return idx;
    }

    /**
     * Fetch all done and not cancelled results from futures into results.
     *
     * This does not wait, it only collects if {@link Future#isDone()} is true and {@link Future#isCancelled()} is false.
     *
     * @param futures A list of futures in an unknown state.
     * @param offset Offset into futures to start at.
     * @param results The results we want to append to.
     * @param unDone Any future that was not complete is put in the unDone list to be handled later.
     * @param <T> The type of the result.
     */
    public static <T> void getReady(final List<Future<T>> futures, final int offset, final List<T> results, final List<Future<T>> unDone) {
        for (int idx = offset; idx < futures.size(); ++idx) {
            // Reap any ready futures.
            for ( ++idx; idx < futures.size(); ++idx) {
                final Future<T> t = futures.get(idx);

                if (!t.isDone()) {
                    unDone.add(t);
                }
                else if (!t.isCancelled()) {
                    try {
                        results.add(t.get());
                    } catch (InterruptedException e1) {
                        // Nop
                    } catch (ExecutionException e1) {
                        // Nop
                    }
                }
            }

        }
    }
}
