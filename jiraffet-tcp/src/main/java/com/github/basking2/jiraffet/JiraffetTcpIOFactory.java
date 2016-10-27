package com.github.basking2.jiraffet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Logic to find and construct a possible {@link JiraffetTcpIO} instance.
 */
public class JiraffetTcpIOFactory {

    public static JiraffetTcpIO buildIo(String thisNode, String[] allNodes) throws IOException {

        // Trim all node names.
        for (int i = 0; i < allNodes.length; ++i) {
            allNodes[i] = allNodes[i].trim();
        }

        if ("auto".equals(thisNode)) {

            IOException lastException = null;

            for (final String node : allNodes) {
                try {
                    return tryBuildIo(node, allNodes);
                }
                catch (final IOException e) {
                    lastException = e;
                }
            }

            throw lastException;
        } else {
            return tryBuildIo(thisNode, allNodes);
        }
    }

    /**
     * Try to builde a {@link JiraffetTcpIO} binding {@code thisNode} as the listening ip.
     *
     * The array of {@code nodes} is copied excluding any values that match {@code thisNode}'s value.
     *
     * @param thisNode This nodes proposed IP and port.
     * @param nodes The list of all known nodes, maybe including {@code thisNode}.
     * @return An IO object.
     * @throws IOException On bind errors.
     */
    private static JiraffetTcpIO tryBuildIo(final String thisNode, final String[] nodes) throws IOException {
        final List<String> otherNodes = new ArrayList<String>(nodes.length);

        for (final String node : nodes) {
            if (!node.equals(thisNode)) {
                otherNodes.add(node);
            }
        }

        return new JiraffetTcpIO(thisNode, otherNodes);
    }
}
