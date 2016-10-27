package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.jiraffet.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class JiraffetTcpIO extends AbstractJiraffetIO implements AutoCloseable {
    
    /**
     * Length limit on IDs.
     */
    public static final int ID_LIMIT = 1000;

    private static final Logger LOG = LoggerFactory.getLogger(JiraffetTcpIO.class);
    private Selector selector;
    private List<String> nodes;
    private Map<String, WritableByteChannel> writableByteChannels;
    private LinkedBlockingQueue<Message> messages;
    private ServerSocketChannel serverSocketChannel;
    private String nodeId;
    private int connectTimeoutMs;

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
        this.serverSocketChannel = ServerSocketChannel.open().bind(toSocketAddress(listen));
        this.serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.nodeId = listen;
        this.connectTimeoutMs = 5000;

    }

    public static SocketAddress toSocketAddress(final String string) {
        final URI uri = URI.create(string);

        if (!uri.getScheme().equals("jiraffet")) {
            throw new IllegalArgumentException("Scheme must be \"jiraffet\".");
        }

        return new InetSocketAddress(uri.getHost(), uri.getPort());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Message> getMessages(long timeout, TimeUnit timeunit) throws JiraffetIOException, TimeoutException, InterruptedException {
        final Timer timer = new Timer(timeout, timeunit);

        final List<Message> msg = new ArrayList<>(messages.size());

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
            receiveMessages(timer);
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
    private void receiveMessages(final Timer timer) throws InterruptedException, JiraffetIOException {

        for (
                final Iterator<SelectionKey> itr = selector.selectedKeys().iterator();
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
                }
                else if (key.isReadable()) {
                    LOG.debug("Reading from {}", key);
                    receiveMessages(
                            timer,
                            (ReadableByteChannel) key.channel(),
                            (KeySelectionAttachment) key.attachment()
                    );
                }
                else if (key.isAcceptable()) {
                    LOG.debug("Accepting from {}", key);
                    final ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
                    final SocketChannel socketChannel = serverSocketChannel.accept();
                    if (socketChannel != null) {
                        socketChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
                        final String id = receiveHandshake(socketChannel);
                        registerChannel(id, socketChannel, true);
                    }
                } else {
                    LOG.error("Unhandled key: {}", key);
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
     * Do the work of putting the channel in the receive selector and in the sending map.
     *
     * There are two callers of this method. The first is when the {@link ServerSocketChannel} accepts
     * a new connection. The second is when the user tries to send a message to another node but now
     * {@link WritableByteChannel} exists yet.
     *
     * @param socketChannel The socket to store.
     * @throws IOException On any IO error.
     */
    public void registerChannel(final String id, final SocketChannel socketChannel, boolean inbound) throws IOException {
        LOG.info("This node {} registering connection with remote node {}.", nodeId, id);
        final WritableByteChannel oldOne = writableByteChannels.put(id, socketChannel);
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ, new KeySelectionAttachment(id));
        if (oldOne != null) {
            if (!inbound) {
                throw new RuntimeException("We should never evict a connection we are making outbound.");
            }
            LOG.info("Evicted previous connection for a new inbound connection {}.", id);
            oldOne.close();
        }
    }

    /**
     * Called by {@link #registerChannel(String, SocketChannel, boolean)} to handle selected channels.
     *
     * @param timer How long may this spend waiting for data.
     * @param in Input channel.
     * @param attachment How to store state about the channel.
     * @throws IOException On any fatal IO error.
     * @throws InterruptedException On thread interruption.
     */
    private void receiveMessages(final Timer timer, ReadableByteChannel in, KeySelectionAttachment attachment)
            throws IOException, InterruptedException
    {
        // The in channel is readable becuase it's closed.
        if (!in.isOpen()) {
            LOG.debug("Removed closed channel {}", in);
            writableByteChannels.remove(attachment.id);
            in.close();
            return;
        }

        // If there is room for header data, receive it.
        if (attachment.body == null && attachment.header.position() < 8) {
            final int i = in.read(attachment.header);

            // Fail on end-of-stream.
            if (i == -1) {
                throw new IOException("End of stream.");
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
            throw new IOException("End of stream.");
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
    protected WritableByteChannel getOutputStream(final String id) throws JiraffetIOException {

        if (id.equals(nodeId)) {
            throw new RuntimeException("Node may not connect to itself: "+id);
        }

        if (writableByteChannels.containsKey(id)) {
            final WritableByteChannel wbc = writableByteChannels.get(id);

            if (wbc.isOpen()) {
                return wbc;
            }

            LOG.error("Connection is not open. Rebuilding it.");
        }

        try {
            LOG.error("Opening new connection to {}", id);
            final SocketChannel chan = SocketChannel.open();
            chan.setOption(StandardSocketOptions.TCP_NODELAY, true);

            final SocketAddress addr = toSocketAddress(id);
            chan.socket().connect(addr, connectTimeoutMs);
            sendHandshake(chan);
            registerChannel(id, chan, false);
            return chan;
        }
        catch (final IOException e) {
            LOG.error("Failed connecting from {} to {}.", nodeId, id, e);
            writableByteChannels.remove(id);
            return null;
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

    /**
     * When a new connection is formed, the client will send a handshake.
     *
     * @see #receiveHandshake(ReadableByteChannel)
     */
    public void sendHandshake(final WritableByteChannel out) throws IOException {
        final int len = 4 + nodeId.getBytes().length;
        final ByteBuffer bb = ByteBuffer.allocate(len);

        bb.putInt(len);
        bb.put(nodeId.getBytes());
        bb.position(0);

        while(bb.position() < bb.limit()) {
            final int i = out.write(bb);
            if (i == -1) {
                throw new IOException(("End of stream."));
            }
        }
    }

    /**
     * When a new connection is formed, the server will receive a handshake.
     *
     * @see #sendHandshake(WritableByteChannel)
     */
    public String receiveHandshake(final ReadableByteChannel in) throws IOException {
        final ByteBuffer length = ByteBuffer.allocate(4);

        final int i = in.read(length);
        if (i == -1) {
            throw new IOException("End of stream.");
        }

        if (length.getInt(0)-4 > ID_LIMIT) {
            throw new IOException("Node ID is beyond the acceptable limit: " + (length.getInt(0)-4));
        }

        final ByteBuffer bb = ByteBuffer.allocate(length.getInt(0)-4);

        while (bb.position() < bb.limit()) {
            final int lastRead = in.read(bb);
            if (lastRead == -1) {
                throw new IOException("End of stream");
            }
        }

        return new String(bb.array());
    }

    public String getNodeId() {
        return this.nodeId;
    }

}
