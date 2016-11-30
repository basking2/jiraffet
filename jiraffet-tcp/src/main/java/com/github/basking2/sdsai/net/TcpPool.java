package com.github.basking2.sdsai.net;

import org.apache.ibatis.annotations.Select;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;

/**
 * This class is not completely thread safe as it manipulates a selector's key sets.
 */
public class TcpPool implements AutoCloseable {

    /**
     * Log.
     */
    private static final Logger LOG = LoggerFactory.getLogger(TcpPool.class);

    /**
     * How to convert a node ID into a network address.
     */
    private IdTranslator idTranslator;

    /**
     * A limit on the length of an ID sent during a handshake.
     */
    public static final int ID_LIMIT = 1000;

    private ServerSocketChannel serverSocketChannel;
    private String nodeId;
    private Selector selector;
    private DataHandlerProvider dataHandlerProvider;

    public TcpPool(final String listen) throws IOException {
        this(listen, new DefaultIdTranslator(), (id) -> DataHandler.NOP_HANDLER);
    }

    public TcpPool(
            final String listen,
            final IdTranslator idTranslator,
            final DataHandlerProvider dataHandlerProvider
    ) throws IOException {
        this.idTranslator = idTranslator;
        this.dataHandlerProvider = dataHandlerProvider;
        this.nodeId = listen;
        this.selector = Selector.open();
        this.serverSocketChannel = ServerSocketChannel.open().bind(idTranslator.translate(listen));
        this.serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
    }

    /**
     * Allow the TcpPool to do work on its sockets.
     */
    public void runOnce(final long timeout) throws IOException {
        final int socketCount = selector.select(timeout);

        processSelect(socketCount);
    }

    public void runOnceNow() throws IOException {
        final int socketCount = selector.selectNow();

        processSelect(socketCount);
    }

    private void processSelect(int socketCount) {
        if (socketCount > 0) {
            for (
                final Iterator<SelectionKey> keys = selector.selectedKeys().iterator();
                keys.hasNext();
            ) {
                try {
                    final SelectionKey key = keys.next();

                    if (!key.isValid()) {
                        if (key.attachment() instanceof TcpPoolAttachment) {
                            final TcpPoolAttachment tcpPoolAttachment = (TcpPoolAttachment) key.attachment();
                            if (tcpPoolAttachment.partialUserKey.dataHandler != null) {
                                tcpPoolAttachment.partialUserKey.dataHandler.handleClose(tcpPoolAttachment.id);
                            }
                        } else {
                            final UserKeyAttachment userKeyAttachment = (UserKeyAttachment) key.attachment();
                            userKeyAttachment.dataHandler.handleClose(userKeyAttachment.id);
                        }

                        continue;
                    }

                    if (key.isConnectable()) {
                        final SocketChannel sock = (SocketChannel) key.channel();
                        final TcpPoolAttachment tcpPoolAttachment = (TcpPoolAttachment) key.attachment();

                        sock.finishConnect();
                        tcpPoolAttachment.handshake = ByteBuffer.wrap(nodeId.getBytes());

                        // Register for write.
                        sock.register(selector, SelectionKey.OP_WRITE, tcpPoolAttachment);
                        continue;
                    }

                    if (key.isAcceptable()) {
                        final ServerSocketChannel serverChan = (ServerSocketChannel) key.channel();

                        final SocketChannel chan = serverChan.accept();

                        if (chan != null) {
                            chan.setOption(StandardSocketOptions.TCP_NODELAY, true);
                            chan.configureBlocking(false);
                            chan.register(selector, SelectionKey.OP_READ, new TcpPoolAttachment("[no id yet]"));
                        }
                        continue;
                    }

                    if (key.isWritable()) {
                        final SocketChannel sock = (SocketChannel) key.channel();
                        if (key.attachment() instanceof TcpPoolAttachment) {
                            handleTcpPoolWrite(sock, key);
                        } else {
                            handleUserWrite(sock, key);
                        }
                    }

                    if (key.isReadable()) {
                        final SocketChannel sock = (SocketChannel) key.channel();
                        if (key.attachment() instanceof TcpPoolAttachment) {
                            handleTcpPoolRead(sock, key);
                        } else {
                            handleUserRead(sock, key);
                        }
                    }

                }
                catch (final IOException e){
                    LOG.error(e.getMessage(), e);
                }
                finally {
                    keys.remove();
                }
            }
        }

    }

    private void handleTcpPoolWrite(final SocketChannel sock, final SelectionKey key) throws IOException {
        final TcpPoolAttachment tcpPoolAttachment = (TcpPoolAttachment) key.attachment();

        if (tcpPoolAttachment.len.position() < tcpPoolAttachment.len.limit()) {
            sock.write(tcpPoolAttachment.len);
        }

        if (tcpPoolAttachment.len.position() >= tcpPoolAttachment.len.limit()) {
            // If writable, we're sending the handshake.
            sock.write(tcpPoolAttachment.handshake);

            // If we've written to our limit, cancel the write key and register as ready.
            if (tcpPoolAttachment.isHandshakeDone()) {
                passOwnershipToListeners(key, tcpPoolAttachment, sock);
            }
        }
    }

    private void handleTcpPoolRead(final SocketChannel sock, final SelectionKey key)  throws IOException {
        final TcpPoolAttachment tcpPoolAttachment = (TcpPoolAttachment) key.attachment();

        // If handshake is null, we didn't get a handshake length yet.
        if (tcpPoolAttachment.handshake == null) {
            sock.read(tcpPoolAttachment.len);

            if (tcpPoolAttachment.len.position() >= 4) {

                int len = tcpPoolAttachment.len.getInt(0);

                if (len > ID_LIMIT) {
                    key.cancel();
                    sock.close();
                    throw new IOException("ID Limit exceeded: " + len + " > " + ID_LIMIT);
                }

                tcpPoolAttachment.handshake = ByteBuffer.allocate(len);
            }
        }

        if (tcpPoolAttachment.handshake != null) {
            sock.read(tcpPoolAttachment.handshake);

            // If we've received up to the limit of our handshake buffer, take action.
            if (tcpPoolAttachment.isHandshakeDone()) {
                tcpPoolAttachment.id = new String(tcpPoolAttachment.handshake.array());
                passOwnershipToListeners(key, tcpPoolAttachment, sock);
            }
        }
    }

    private void handleUserWrite(final SocketChannel sock, final SelectionKey key) throws IOException {
        final UserKeyAttachment userKeyAttachment = (UserKeyAttachment)key.attachment();

        while (userKeyAttachment.sendQueue.size() > 0) {
            final ByteBuffer bb = userKeyAttachment.sendQueue.peek();
            final int lastWritten = sock.write(bb);

            if (lastWritten == -1) {
                // End of stream?
                userKeyAttachment.dataHandler.handleClose(userKeyAttachment.id);
                userKeyAttachment.key.cancel();
                LOG.error("Failed to write to host {}", userKeyAttachment.id);
                return;
            }

            // Did we enqueue one segment completely? If so, remove it.
            if (bb.position() == bb.limit()) {
                userKeyAttachment.sendQueue.poll();
            }
        }

        // When there is no data left to send, register for only reads.
        key.interestOps(SelectionKey.OP_READ);
    }

    private void handleUserRead(final SocketChannel sock, final SelectionKey key) throws IOException {
        final UserKeyAttachment userKeyAttachment = (UserKeyAttachment)key.attachment();
        userKeyAttachment.dataHandler.handleData(userKeyAttachment.id, sock);
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

    public UserKeyAttachment connect(final String id) throws IOException {
        if (id.equals(nodeId)) {
            throw new IOException("Node may not connect to itself: "+id);
        }

        // Search if we already have a connection we're working on.
        for (final SelectionKey key : selector.keys()) {
            if (key.attachment() == null) {
                continue;
            }

            if (key.attachment() instanceof TcpPoolAttachment) {
                final TcpPoolAttachment tcpPoolAttachment = (TcpPoolAttachment) key.attachment();
                if (id.equals(tcpPoolAttachment.id)) {
                    return tcpPoolAttachment.partialUserKey;
                }
            }
            else {
                final UserKeyAttachment userKeyAttachment = (UserKeyAttachment) key.attachment();
                if (id.equals(userKeyAttachment.id)) {
                    return userKeyAttachment;
                }
            }
        }

        final SocketAddress addr = idTranslator.translate(id);
        final SocketChannel chan = SocketChannel.open();

        chan.setOption(StandardSocketOptions.TCP_NODELAY, true);
        chan.configureBlocking(false);
        final TcpPoolAttachment attachment = new TcpPoolAttachment(id);
        chan.register(selector, SelectionKey.OP_CONNECT, attachment);
        chan.connect(addr);

        return attachment.partialUserKey;
    }

    public static class DefaultIdTranslator implements IdTranslator {
        @Override public SocketAddress translate(String id) {
            final URI uri = URI.create(id);

            if (!uri.getScheme().equals("tcp")) {
                throw new IllegalArgumentException("Scheme must be \"tcp\".");
            }

            return new InetSocketAddress(uri.getHost(), uri.getPort());
        }
    }

    /**
     * A {@link SelectionKey} attachment used by the internals of {@link TcpPool}.
     */
    private static class TcpPoolAttachment {
        public String id;
        public ByteBuffer len;
        public ByteBuffer handshake;
        public CompletableFuture<SocketChannel> future;
        public UserKeyAttachment partialUserKey;


        public TcpPoolAttachment(String id) {
            this.id = id;
            this.len = ByteBuffer.allocate(4);
            this.len.putInt(0, id.getBytes().length);
            this.future = new CompletableFuture<>();
            this.partialUserKey = new UserKeyAttachment(id, null, null);
        }

        public boolean isHandshakeDone() {
            return (handshake != null) && (handshake.position() >= handshake.limit());
        }

        /**
         * Used a single time, this will complete the partial {@link UserKeyAttachment} and return it.
         *
         * @param key They key.
         * @param dataHandlerProvider How to fetch a {@link DataHandler}.
         * @return The completed key.
         * @throws IllegalStateException on subsequent calls.
         */
        public UserKeyAttachment completeUserKey(final SelectionKey key, final DataHandlerProvider dataHandlerProvider) throws IOException {
            if (this.partialUserKey == null) {
                throw new IllegalStateException("CompleteUserKey has already been called.");
            }
            final UserKeyAttachment uka = this.partialUserKey;
            // Safety assignment. Provoke early NPEs.
            this.partialUserKey = null;

            uka.key = key;
            uka.dataHandler = dataHandlerProvider.getDataHandler(id);

            // All future data calls go to the user, not the TcpPoolAttachment.
            key.attach(uka);

            // If nothing to send, register for only reads. Otherwise, reads and writes.
            if (uka.sendQueue.size() == 0) {
                key.interestOps(SelectionKey.OP_READ);
            }
            else {
                key.interestOps(SelectionKey.OP_READ | SelectionKey.OP_WRITE);
            }

            // Signal the user that we are officially open!
            uka.dataHandler.handleOpen(uka.id);

            return uka;
        }
    }

    /**
     * A {@link SelectionKey} attachment used to communicate with the user.
     *
     * @see {@link TcpPoolAttachment#completeUserKey(SelectionKey, DataHandlerProvider)}.
     */
    public static class UserKeyAttachment {
        private String id;
        private DataHandler dataHandler;
        private ConcurrentLinkedQueue<ByteBuffer> sendQueue = new ConcurrentLinkedQueue<>();
        private SelectionKey key;

        /**
         * A user attachment.
         * @param key The selection key.
         * @param dataHandler A method for the user to handle data. Generated by a call to {@link DataHandlerProvider}
         *                    which the user should provide to us.
         */
        public UserKeyAttachment(final String id, final SelectionKey key, final DataHandler dataHandler) {
            this.id = id;
            this.dataHandler = dataHandler;
            this.key = key;
        }

        /**
         * Make a partial user key. A paritial user key attachment has key and dataHandler set to null.
         *
         * It can be used to queue messages for sending later, but not much else.
         *
         * @param id
         * @see {@link TcpPoolAttachment#completeUserKey(SelectionKey, DataHandlerProvider)}.
         */
        private UserKeyAttachment(final String id) {
            this(id, null, null);
        }

        /**
         * Shedule data to be sent as this channel is able to be written to.
         *
         * @param segments The data segments to be written.
         */
        public void write(final ByteBuffer ... segments) {
            for (final ByteBuffer segment : segments) {
                sendQueue.add(segment);
            }

            // If we are just now starting to be interested in sending, register our interest.
            if (sendQueue.size() == segments.length && key != null) {
                key.interestOps(SelectionKey.OP_WRITE | SelectionKey.OP_WRITE);
            }
        }
    }


    private void passOwnershipToListeners(final SelectionKey key, final TcpPoolAttachment attachment, final SocketChannel chan) throws IOException {
        LOG.debug("Passing ownership of {}.", attachment.id);

        attachment.completeUserKey(key, dataHandlerProvider);
    }

    public interface DataHandler {
        static final DataHandler NOP_HANDLER = new DataHandler() {
            @Override
            public void handleOpen(String id) throws IOException {
                // Nop
            }

            @Override
            public void handleData(String id, SocketChannel chan) throws IOException {
                // Nop
            }

            @Override
            public void handleClose(String id) throws IOException {
                // Nop
            }
        };
        void handleOpen(String id) throws IOException;
        void handleData(String id, SocketChannel chan) throws IOException;
        void handleClose(String id) throws IOException;
    }

    /**
     * The {@link DataHandlerProvider} is called when a new connection to an id is established.
     *
     * Any aggregated data should be flushed and the connections state be reinitialized.
     *
     * In this sense, the lifetime of the {@link DataHandler} returned by this interface mimics the lifetime of the
     * connection.
     */
    @FunctionalInterface
    public interface DataHandlerProvider {
        DataHandler getDataHandler(String id);
    }

    @FunctionalInterface
    public interface IdTranslator {
        SocketAddress translate(String id);
    }
}
