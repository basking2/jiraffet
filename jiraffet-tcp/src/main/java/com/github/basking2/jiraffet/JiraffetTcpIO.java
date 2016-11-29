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
    final private Map<String, SocketChannel> writableByteChannels;
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

        final TcpPool.SocketHandler socketHandler = new TcpPool.SocketHandler() {
            @Override
            public void handleNewSocket(final String id, final SocketChannel socketChannel)
            {
                final WritableByteChannel wbc = writableByteChannels.put(id, socketChannel);
                LOG.info("Registering {}. Curr {}, Prev {}", id, socketChannel, wbc);
                if (wbc == socketChannel) {
                    LOG.warn("Socked re-added to live list. Internal logic error?");
                }
                else if (wbc != null) {
                    try {
                        LOG.warn("Evicting previous socket for {}", id);
                        wbc.close();
                    }
                    catch (final IOException e) {
                        LOG.warn("Closing old socket {}.", id, e);
                    }
                }


                try {
                    socketChannel.register(selector, SelectionKey.OP_READ, new KeySelectionAttachment(id));
                } catch (ClosedChannelException e) {
                    LOG.warn("Socket closed while registering. {}.", id, e);
                }

            }
        };

        this.tcpPool = new AppTcpPool(nodeId, "jiraffet", socketHandler);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> getMessages(long timeout, TimeUnit timeunit) throws JiraffetIOException, TimeoutException, InterruptedException {
        final Timer timer = new Timer(timeout, timeunit);

        final List<Message> msg = new ArrayList<>(messages.size());

        try {
            tcpPool.runOnceNow();
        } catch (final IOException e) {
            throw new JiraffetIOException(e);
        }

        final int numReady;
        try {
            numReady = selector.select(timer.remaining());
            LOG.debug("Sockets ready: {}", numReady);
        }
        catch (final IOException e) {
            LOG.error("FATAL", e);
            throw new JiraffetIOException(e);
        }

        if (numReady > 0) {
            selectSockets(timer);
        }

        final Message firstMessage = messages.poll(timer.remaining(), Timer.TIME_UNIT);
        if (firstMessage == null) {
            throw new TimeoutException("Waiting for messages.");
        }

        msg.add(firstMessage);
        messages.drainTo(msg);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Returning {} messages from getMessages()", msg.size());
            for (final Message m : msg) {
                LOG.debug("\tMessage: {}", m);
            }
        }

        return msg;
    }

    /**
     * Called by {@link #getMessages(long, TimeUnit)} with a {@link Timer} to limit the waiting and work done.
     *
     * @param timer Timer to limit the work and waiting.
     * @throws InterruptedException On thread interruption.
     * @throws JiraffetIOException On an error that will stop any future progress.
     */
    private void selectSockets(final Timer timer) throws InterruptedException, JiraffetIOException {

        final Set<SelectionKey> selectionKeys = selector.selectedKeys();

        for (
                final Iterator<SelectionKey> itr = selectionKeys.iterator();
                itr.hasNext();
        )
        {
            final SelectionKey key = itr.next();
            final KeySelectionAttachment keySelectionAttachment = (KeySelectionAttachment)key.attachment();
            try {
                if (!key.isValid()) {
                    // This occurs when 1) a key is selected, 2) the remote host closes it.
                    LOG.debug("Ignoring invalid key.");
                    key.cancel();
                    continue;
                }

                if (!key.channel().isOpen()) {
                    LOG.info("Channel is closed to {}", keySelectionAttachment.id);
                    key.cancel();
                    continue;
                }

                if (key.isReadable()) {
                    LOG.debug("Reading from {}", key);
                    receiveMessages(
                            timer,
                            (ReadableByteChannel) key.channel(),
                            (KeySelectionAttachment) key.attachment()
                    );
                }

                if (key.isWritable()) {
                    final KeySelectionAttachment attachment = (KeySelectionAttachment) key.attachment();
                    if (attachment.sendList.size() == 0) {
                        // We managed to send everything!
                        // Re-register to only get READ events.
                        key.interestOps(SelectionKey.OP_READ);
                    }
                    else {
                        LOG.debug("Writing to {}", attachment.id);

                        sendMessages(
                                timer,
                                (WritableByteChannel) key.channel(),
                                attachment
                        );
                    }
                }
            }
            catch (final IOException e) {
                LOG.debug("ID: {}", keySelectionAttachment.id);
                LOG.debug("Channel: {}", key.channel());
                handleException(keySelectionAttachment.id, "receiving data", (SocketChannel)key.channel(), e);
            }
            finally {
                // Remove handled key from the selection set.
                itr.remove();
            }
        }
    }

    /**
     * @param timer How long may this spend waiting for data.
     * @param out Input channel.
     * @param attachment How to store state about the channel.
     * @throws IOException On any fatal IO error.
     * @throws InterruptedException On thread interruption.
     */
    private void sendMessages(final Timer timer, WritableByteChannel out, KeySelectionAttachment attachment)
            throws IOException, InterruptedException
    {
        // The in channel is readable because it's closed.
        if (!out.isOpen()) {
            LOG.debug("Removed closed channel {}", out);
            writableByteChannels.remove(attachment.id);
            return;
        }

        // While we have data to send and time remaining.
        while (attachment.sendList.size() > 0 && timer.remainingNoThrow() > 0) {

            // Get the front of the buffer.
            final ByteBuffer sendMe = attachment.sendList.peek();

            // Do a send.
            int sent = out.write(sendMe);
            if (sent < 0) {
                // Error!?
                writableByteChannels.remove(attachment.id);
                out.close();
                break;
            }

            if (sendMe.position() >= sendMe.limit()) {
                attachment.sendList.poll();
            }
        }
    }

    /**
     * @param timer How long may this spend waiting for data.
     * @param in Input channel.
     * @param attachment How to store state about the channel.
     * @throws IOException On any fatal IO error.
     * @throws InterruptedException On thread interruption.
     */
    private void receiveMessages(final Timer timer, ReadableByteChannel in, KeySelectionAttachment attachment)
            throws IOException, InterruptedException
    {
        // The in channel is readable because it's closed.
        if (!in.isOpen()) {
            LOG.debug("Removed closed channel {}", in);
            writableByteChannels.remove(attachment.id);
            return;
        }

        // If there is room for header data, receive it.
        if (attachment.body == null && attachment.header.position() < 8) {
            final int i = in.read(attachment.header);

            // Fail on end-of-stream.
            if (i == -1) {
                throw new IOException("End of stream, header.");
            }

            // If we still don't have the header, leave.
            if (attachment.header.position() < 8) {
                return;
            }
        }

        // If we are here and have no body yet, create one. We have the length.
        if (attachment.body == null) {
            // This can be larger than len if the limit is set.
            attachment.body = ByteBuffer.allocate(attachment.getLen()-8);
            // attachment.body.limit(attachment.getLen());
        }

        final int i = in.read(attachment.body);
        if (i == -1) {
            throw new IOException("End of stream, body.");
        }

        // If we've reached the limit, we're done.
        if (attachment.body.position() == attachment.body.limit()) {
            final Message m;
            try {
                m = jiraffetProtocol.unmarshal(attachment.header, attachment.body);
            }
            catch (final IllegalArgumentException e) {
                throw new IOException("Decoding received message", e);
            }
            attachment.clear();

            if (m == null) {
                throw new RuntimeException("Trying to enqueue a NULL message.");
            }
            messages.put(m);
        }
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
            final SocketChannel wbc = writableByteChannels.get(id);

            if (wbc.isOpen()) {
                final SelectionKey selectionKey = wbc.keyFor(selector);
                if (selectionKey != null) {
                    final KeySelectionAttachment keySelectionAttachment = (KeySelectionAttachment)selectionKey.attachment();
                    if (keySelectionAttachment != null) {
                        write(keySelectionAttachment, selectionKey, segments);
                    }
                }
                return;
            }

            LOG.error("Connection is not open. Rebuilding it.");
        }

        try {
            LOG.info("Opening new connection to {}", id);

            tcpPool.connect(id);
        }
        catch (final IOException e) {
            LOG.error("Failed connecting from {} to {}.", nodeId, id, e);
            writableByteChannels.remove(id);
        }
    }

    protected void write(
            final KeySelectionAttachment keySelectionAttachment,
            final SelectionKey selectionKey,
            final ByteBuffer ... segments
    ) throws JiraffetIOException {
        if (keySelectionAttachment.sendList.size() + segments.length > 1000) {
            LOG.error("Send queue limit exceeded. Dropping messages to node "+keySelectionAttachment.id);
        }

        for (final ByteBuffer segment : segments) {
            keySelectionAttachment.sendList.add(segment);
        }

        // Enable READ select if we added to an empty list.
        if (segments.length == keySelectionAttachment.sendList.size()) {
            selectionKey.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
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

    public static class KeySelectionAttachment {

        public String id;

        /**
         * Header described by {@link JiraffetProtocol#unmarshal(ByteBuffer, ByteBuffer)}.
         */
        public ByteBuffer header;

        /**
         * Body described by {@link JiraffetProtocol#unmarshal(ByteBuffer, ByteBuffer)}.
         */
        public ByteBuffer body;

        public int getLen() {
            return header.getInt(0);
        }

        /**
         * List of buffers to send as soon as possible.
         */
        public ConcurrentLinkedQueue<ByteBuffer> sendList;

        /**
         * Clear the receiving message state.
         *
         * This is done when a message is fully received.
         *
         * The sending queue is never cleared.
         */
        public void clear() {
            header.putInt(0, 0);
            header.putInt(0, 4);
            header.position(0);
            header.limit(8);
            body = null;
        }

        public KeySelectionAttachment(final String id) {
            this.id = id;
            this.header = ByteBuffer.allocate(8);
            this.body = null;
            this.sendList = new ConcurrentLinkedQueue<>();
        }
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
}
