package com.github.basking2.jiraffetdb.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility that tracks the highest version something, as voted by a majority.
 */
public class VersionVoter {

    /**
     * The highest current version that has a majority of votes.
     */
    private int currentVersion;

    /**
     * A map from version numbers to vote totals.
     */
    private Map<Integer, Integer> votes;

    /**
     * A map from version numbers to rejection totals.
     */
    private Map<Integer, Integer> rejections;

    /**
     * A map from version numbers to listeners.
     */
    private Map<Integer, VersionListener> listeners;

    /**
     * How many voters are in this election.
     */
    private int voters;

    public VersionVoter(final int voters) {
        this.votes = new HashMap<>();
        this.listeners = new HashMap<>();
        this.rejections = new HashMap<>();
        this.voters = voters;
        this.currentVersion = 0;
    }

    public void reject(int version) {
        int rejectTotal = votes.getOrDefault(version, 0) + 1;

        // Update that total.
        rejections.put(version, rejectTotal);

        // If a version is utterly rejected, tell the client.
        if (rejectTotal > voters / 2) {
            notifyOfNewVersion(version, false);
        }
    }

    /**
     * Vote for the current version and, implicitly, for all preceeding versions.
     *
     * If any version gains a majority and is higher than the current version, it's listener (if any) is notified.
     *
     * @param version The version to vote for.
     */
    public void vote(int version) {
        int voteTotal = votes.getOrDefault(version, 0) + 1;

        // Update that total.
        votes.put(version, voteTotal);

        // If we have a new highest vote total, adjust our stored voters to save space.
        if (voteTotal > voters / 2 && version > currentVersion) {
            newVersion(version);
        }
        else {
            voteForLowerVersions(version);
        }
    }

    /**
     * Return the current version.
     * @return the current version.
     */
    public int getCurrentVersion() {
        return currentVersion;
    }

    /**
     * Set a listener for a paricular version.
     *
     * @param version The version to report on if it wins or loses the election.
     * @param listener The listener to notify.
     */
    public void setListener(int version, final VersionListener listener) {
        listeners.put(version, listener);
    }

    /**
     * Vote for version below this one.
     *
     * @param version The version for which lower versions should be voted for.
     */
    private void voteForLowerVersions(int version) {

        int highestWinner = currentVersion;

        // For all keys.
        for (int k : votes.keySet()) {
            // If the key is less than the version we are considering, vote for it.
            if (k < version) {
                int v = votes.get(k) + 1;
                votes.put(k, v);

                // If that vote makes the lower key a winning key, note as much.
                if (v > voters / 2 && v > highestWinner) {
                    highestWinner = v;
                }
            }
        }

        if (highestWinner > currentVersion) {
            newVersion(highestWinner);
        }
    }

    private void newVersion(int version) {
        currentVersion = version;

        notifyOfNewVersion(version, true);

        removeVeryLowVersions();
    }

    /**
     * Remove all versions that are less than the current version.
     */
    private void removeVeryLowVersions() {

        for (int k : new ArrayList<Integer>(votes.keySet())) {
            // NOTE: Since currentVersion has a mojority of votes, it can never again grow to a majority of votes.
            if (k <= currentVersion) {
                votes.remove(k);
                rejections.remove(k);
            }
        }
    }

    private void notifyOfNewVersion(int version, boolean success) {
        final VersionListener l = listeners.remove(version);
        if (l != null) {
            l.success(version, success);
        }
    }

    /**
     * Sets the current number of voters.
     *
     * It is save to increase this, but decreasing it may delay detection of a new winner.
     *
     * It is best to clear this datastructure before setting votes.
     *
     * @param voters The number of votes to compute a majority from.
     */
    public void setVoters(int voters) {
        this.voters = voters;
    }

    public void clear() {
        for (Map.Entry<Integer, VersionListener> entry : listeners.entrySet()) {
            entry.getValue().success(entry.getKey(), false);
        }

        currentVersion = 0;
        votes.clear();
        rejections.clear();
        listeners.clear();
    }

    @FunctionalInterface
    public interface VersionListener {

        /**
         * Report to a listener if a vote was successful or not.
         * @param version The version reported on.
         * @param success Was the version voted (true) or rejected (false)?
         */
        void success(int version, boolean success);
    }

}
