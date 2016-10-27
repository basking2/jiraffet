package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.jiraffet.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 */
public class JiraffetTcpIO extends AbstractJiraffetIO implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(JiraffetTcpIO.class);
    private Selector selector;
    private List<String> nodes;
    private Map<String, WritableByteChannel> writableByteChannels;
    private LinkedBlockingQueue<Message> messages;
    private ServerSocketChannel serverSocketChannel;
    private String nodeId;

    public JiraffetTcpIO(final String listen, final List<String> nodes) throws IOException {
        this.selector = Selector.open();
        this.nodes = nodes;
        this.writableByteChannels = new HashMap<>();
        this.messages = new LinkedBlockingQueue<>();
        this.serverSocketChannel = ServerSocketChannel.open().bind(toSocketAddress(listen));
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.nodeId = listen;
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
        }
        catch (final IOException e) {
            throw new JiraffetIOException(e);
        }

        if (numReady > 0) {
            receiveMessages(timer);
        }

        msg.add(messages.poll(timer.remaining(), Timer.TIME_UNIT));
        messages.drainTo(msg);

        return msg;
    }

    /**
     * Called by {@link #getMessages(long, TimeUnit)} with a {@link Timer} to limit the waiting and work done.
     *
     * @param timer Timer to limit the work and waiting.
     * @throws InterruptedException On thread interruption.
     * @throws JiraffetIOException On an error that will stop any future progress.
     */
    public void receiveMessages(final Timer timer) throws InterruptedException, JiraffetIOException {
        for (final SelectionKey key : selector.selectedKeys()) {
            KeySelectionAttachment keySelectionAttachment = (KeySelectionAttachment)key.attachment();
            try {
                if (key.isReadable()) {
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
                        final String id = receiveHandshake(socketChannel);
                        registerChannel(id, socketChannel);
                    }
                }
            } catch (final IOException e) {
                handleException(keySelectionAttachment.id, "receiving data", (SocketChannel)key.channel(), e);
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
    public void registerChannel(final String id, final SocketChannel socketChannel) throws IOException {
        LOG.info("Registering connection of {} with remote node {}.", nodeId, id);
        final WritableByteChannel oldOne = writableByteChannels.put(id, socketChannel);
        socketChannel.configureBlocking(false);
        socketChannel.register(selector, SelectionKey.OP_READ, new KeySelectionAttachment(id));
        if (oldOne != null) {
            oldOne.close();
        }
    }

    /**
     * Called by {@link #registerChannel(String, SocketChannel)} to handle selected channels.
     *
     * @param timer How long may this spend waiting for data.
     * @param in Input channel.
     * @param attachment How to store state about the channel.
     * @throws IOException On any fatal IO error.
     * @throws InterruptedException On thread interruption.
     */
    public void receiveMessages(final Timer timer, ReadableByteChannel in, KeySelectionAttachment attachment)
            throws IOException, InterruptedException
    {
        if (attachment.header.position() < 8) {
            in.read(attachment.header);

            // If we still don't have the header, leave.
            if (attachment.header.position() < 8) {
                return;
            }
        }

        if (attachment.body == null) {
            // This can be larger than len if the limit is set.
            attachment.body = ByteBuffer.allocate(attachment.getLen());
            // attachment.body.limit(attachment.getLen());
        }

        in.read(attachment.body);

        // If we've reached the limit, we're done.
        if (attachment.body.position() == attachment.body.limit()) {
            final Message m = jiraffetProtocol.unmarshal(attachment.header, attachment.body);
            attachment.clear();
            messages.put(m);
        }
    }

    @Override
    public List<String> nodes() {
        return nodes;
    }

    @Override
    protected WritableByteChannel getOutputStream(String id) throws JiraffetIOException {

        final SocketAddress addr = toSocketAddress(id);
        if (writableByteChannels.containsKey(addr)) {
            return writableByteChannels.get(addr);
        }
        else {
            try {
                final SocketChannel chan = SocketChannel.open(addr);
                sendHandshake(chan);
                registerChannel(id, chan);
                return chan;
            }
            catch (final IOException e) {
                LOG.error("Connecting node {} to {}.", nodeId, id, e);
                return null;
            }
        }
    }

    @Override
    protected void handleException(
            final String nodeId,
            final String action,
            final WritableByteChannel out,
            final IOException e
    ) throws JiraffetIOException {
        final SocketChannel wbc = (SocketChannel) writableByteChannels.remove(nodeId);
        try {
            wbc.close();
        }
        catch (final IOException e2) {
            LOG.warn("Failed to close channel to {}: {}", nodeId, e2.getMessage());
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


        while(bb.position() < bb.limit()) {
            out.write(bb);
        }
    }

    /**
     * When a new connection is formed, the server will receive a handshake.
     *
     * @see #sendHandshake(WritableByteChannel)
     */
    public String receiveHandshake(final ReadableByteChannel in) throws IOException {
        final ByteBuffer length = ByteBuffer.allocate(4);
        in.read(length);

        final ByteBuffer bb = ByteBuffer.allocate(length.getInt(0));

        while (bb.position() < bb.limit()) {
            in.read(bb);
        }

        return new String(bb.array());
    }
}
