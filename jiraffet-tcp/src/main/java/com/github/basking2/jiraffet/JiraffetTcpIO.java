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
    private Map<URI, WritableByteChannel> writableByteChannels;
    private LinkedBlockingQueue<Message> messages;

    public JiraffetTcpIO(final SocketAddress listen, final List<String> nodes) throws IOException {
        this.selector = Selector.open();
        this.nodes = nodes;
        this.writableByteChannels = new HashMap<>();
        this.messages = new LinkedBlockingQueue<>();

        ServerSocketChannel chan = ServerSocketChannel.open().bind(listen);
        chan.configureBlocking(false);
        chan.register(selector, SelectionKey.OP_ACCEPT);
    }

    @Override
    public List<Message> getMessages(long timeout, TimeUnit timeunit) throws IOException, TimeoutException, InterruptedException {
        final Timer timer = new Timer(timeout, timeunit);

        List<Message> msg = new ArrayList<>(messages.size());

        int numReady = selector.select(timer.remaining());
        if (numReady > 0) {
            receiveMessages(timer);
        }

        messages.add(messages.poll(timer.remaining(), Timer.TIME_UNIT));
        messages.drainTo(msg);

        return msg;
    }

    public void receiveMessages(final Timer timer) throws InterruptedException, IOException {
        for (final SelectionKey key : selector.selectedKeys()) {
            if (key.isReadable()) {
                receiveMessages(
                        timer,
                        (ReadableByteChannel)key.channel(),
                        (KeySelectionAttachment)key.attachment()
                        );
            }
            else if (key.isAcceptable()) {
                // FIXME
            }
        }
    }

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
    protected WritableByteChannel getOutputStream(String id) throws IOException {

        final URI uri = URI.create(id);
        if (writableByteChannels.containsKey(uri)) {
            return writableByteChannels.get(uri);
        }
        else {
            final SocketChannel chan = SocketChannel.open(new InetSocketAddress(uri.getHost(), uri.getPort()));
            writableByteChannels.put(uri, chan);
            return chan;
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
        /** 
         * Header described by {@link JiraffetProtocol#unmarshal(ByteBuffer, ByteBuffer)}.
         */
        public ByteBuffer header = ByteBuffer.allocate(8);
        
        /**
         * Body described by {@link JiraffetProtocol#unmarshal(ByteBuffer, ByteBuffer)}.
         */
        public ByteBuffer body = null;

        public int getLen() {
            return header.getInt(0);
        }

        public void clear() {
            header.position(0);
            header.limit(8);
            body = null;
        }
    }
}
