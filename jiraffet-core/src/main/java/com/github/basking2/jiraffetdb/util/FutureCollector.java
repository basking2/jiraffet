package com.github.basking2.jiraffetdb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * An object for managing a list of {@link Future} objects.
 *
 * This provides logic to sort them into done and undone lists.
 */
public class FutureCollector<T> {
    private List<Future<T>> futures;
    private List<T> results;
    private List<Future<T>> undone;

    public FutureCollector(final List<Future<T>> futures) {
        this.futures = futures;
        this.results = new ArrayList<>(futures.size());
        this.undone = new ArrayList<>(futures.size());
    }

    public void waitAtMost(final long timeout, final TimeUnit timeUnit) {
        Futures.waitAtMost(futures, results, undone, timeout, timeUnit);
    }

    public List<T> getResults() {
        return results;
    }

    public void cancelUndone(boolean mayInterruptIfRunning) {
        for (Future<?> f : undone) {
            f.cancel(mayInterruptIfRunning);
        }
    }

}
