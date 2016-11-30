package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.jiraffetdb.util.Timer;
import com.github.basking2.sdsai.net.AppTcpPool;
import com.github.basking2.sdsai.net.TcpPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * An implementation of {@link AbstractJiraffetIO}.
 *
 * This class is not thread-safe in that there exist race conditions if send and receive operations happen
 * simultaneously.
 */
public class JiraffetTcpIO extends AbstractJiraffetIO implements AutoCloseable {
    
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetTcpIO.class);
    final private Selector selector;
    final private List<String> nodes;
    final private Map<String, TcpPool.UserKeyAttachment> writableByteChannels;
    final private LinkedBlockingQueue<Message> messages;
    final private String nodeId;
    final private TcpPool tcpPool;

    public JiraffetTcpIO(final String listen, final List<String> nodes) throws IOException {

        // Sanity checks.
        for (final String n : nodes) {
            if (n.equals(listen)) {
                throw new IllegalArgumentException("Node's listen address cannot be in the nodes list: "+listen);
            }
        }

        this.selector = Selector.open();
        this.nodes = nodes;
        this.writableByteChannels = new HashMap<>();
        this.messages = new LinkedBlockingQueue<>();
        this.nodeId = listen;

        this.tcpPool = new AppTcpPool(nodeId, "jiraffet", new JiraffetDataHandlerProvider());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> getMessages(long timeout, TimeUnit timeunit) throws JiraffetIOException, TimeoutException, InterruptedException {
        final Timer timer = new Timer(timeout, timeunit);

        final List<Message> msg = new ArrayList<>(messages.size());

        try {
            while (messages.size() == 0) {
                tcpPool.runOnce(timer.remaining());
            }
        } catch (final IOException e) {
            throw new JiraffetIOException(e);
        }

        // Ensure we have at least 1 message, and throw an exception if we don't.
        msg.add(messages.remove());

        // Get any remaining messages.
        messages.drainTo(msg);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Returning {} messages from getMessages()", msg.size());
            for (final Message m : msg) {
                LOG.debug("\tMessage: {}", m);
            }
        }

        return msg;
    }

    @Override
    public List<String> nodes() {
        return nodes;
    }

    @Override
    protected void write(final String id, final ByteBuffer ... segments) throws JiraffetIOException {
        if (id.equals(nodeId)) {
            throw new RuntimeException("Node may not send to itself: "+id);
        }

        if (writableByteChannels.containsKey(id)) {
            final TcpPool.UserKeyAttachment wbc = writableByteChannels.get(id);
            wbc.write(segments);
        }
        else {
            LOG.info("Opening new connection to {}", id);

            try {
                final TcpPool.UserKeyAttachment wbc = tcpPool.connect(id);
                wbc.write(segments);
            }
            catch (final IOException e) {
                LOG.error("Failed connecting from {} to {}.", nodeId, id, e);
                writableByteChannels.remove(id);
            }
        }
    }

    @Override
    protected void handleException(
            final String id,
            final String action,
            final WritableByteChannel out,
            final IOException e
    ) throws JiraffetIOException {
        LOG.error("Failure {} to {}", action, id, e);
        writableByteChannels.remove(id);
        try {
            out.close();
        }
        catch (final IOException e2) {
            LOG.warn("Failed to close channel to {}: {}", id, e2.getMessage());
        }
    }

    @Override
    public void close() throws Exception {

        tcpPool.close();

        for (SelectionKey k : selector.keys()) {
            try {
                k.channel().close();
            } catch (IOException e) {
                LOG.warn("Failed to close channel.", e);
            }
        }

        selector.close();
    }

    @Override
    public void clientRequest(final List<ClientRequest> clientRequests) {
        for (final ClientRequest clientRequest : clientRequests) {
            try {
                messages.put(clientRequest);
            } catch (final InterruptedException e) {
                clientRequest.complete(false, "[unknown]", "Submission to local node failed: "+e.getMessage());
            }
        }
    }

    public String getNodeId() {
        return this.nodeId;
    }

    private class JiraffetDataHandlerProvider implements TcpPool.DataHandlerProvider {

        @Override
        public TcpPool.DataHandler getDataHandler(String id) {
            return new JiraffetDataHandler();
        }
    }

    private class JiraffetDataHandler implements TcpPool.DataHandler {

        private ByteBuffer header = null;
        private ByteBuffer body = null;

        @Override
        public void handleOpen(String id) {
            header = ByteBuffer.allocate(8);
            body = null;
        }

        @Override
        public void handleData(final String id, final SocketChannel chan) throws IOException {
            receiveMessages(id, chan);
        }

        @Override
        public void handleClose(String id) {
            // Make sure we're gone.
            writableByteChannels.remove(id);
        }

        private void receiveMessages(final String id, SocketChannel in) throws IOException
        {
            // The in channel is readable because it's closed.
            if (!in.isOpen()) {
                LOG.debug("Removed closed channel {}", in);
                writableByteChannels.remove(id);
                return;
            }

            // If there is room for header data, receive it.
            if (body == null && header.position() < 8) {
                final int i = in.read(header);

                // Fail on end-of-stream.
                if (i == -1) {
                    writableByteChannels.remove(id);
                    in.close();
                    throw new IOException("End of stream, header.");
                }

                // If we still don't have the header, leave.
                if (header.position() < 8) {
                    return;
                }
            }

            // If we are here and have no body yet, create one. We have the length.
            if (body == null) {
                // This can be larger than len if the limit is set.
                body = ByteBuffer.allocate(getLen()-8);
                // body.limit(attachment.getLen());
            }

            final int i = in.read(body);
            if (i == -1) {
                writableByteChannels.remove(id);
                in.close();
                throw new IOException("End of stream, body.");
            }

            // If we've reached the limit, we're done.
            if (body.position() == body.limit()) {
                final Message m;

                try {
                    m = jiraffetProtocol.unmarshal(header, body);
                }
                catch (final IllegalArgumentException e) {
                    throw new IOException("Decoding received message", e);
                }
                finally {
                    clear();
                }

                if (m == null) {
                    throw new RuntimeException("Trying to enqueue a NULL message.");
                }

                try {
                    messages.put(m);
                }
                catch (final InterruptedException e) {
                    throw new RuntimeException("Unexpected error enqueing message.", e);
                }
            }
        }

        /**
         * Set body to null and clear the header.
         */
        private void clear() {
            header.putInt(0, 0);
            header.putInt(4, 0);
            header.clear();
            body = null;
        }

        public int getLen() {
            return header.getInt(0);
        }
    }
}
