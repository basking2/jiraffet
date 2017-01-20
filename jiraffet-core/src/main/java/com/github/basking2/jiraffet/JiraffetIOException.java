package com.github.basking2.jiraffet;

import java.io.IOException;

/**
 * Because Raft has failure built into the protocol, we only want to ever propogate out JiraffetRaft-specific IO exceptions.
 */
public class JiraffetIOException extends Exception {
    public JiraffetIOException(final IOException e) {
        super(e.getMessage(), e);
    }
}
