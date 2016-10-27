package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;
import com.github.basking2.jiraffet.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.WritableByteChannel;
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

    public JiraffetTcpIO(final List<String> nodes) throws IOException {
        this.selector = Selector.open();
        this.nodes = nodes;
        this.writableByteChannels = new HashMap<>();
        this.messages = new LinkedBlockingQueue<>();
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

    public void receiveMessages(final Timer timer) throws IOException {
        for (final SelectionKey key : selector.selectedKeys()) {
            if (key.isReadable()) {
                receiveMessages(
                        timer,
                        (ReadableByteChannel)key.channel(),
                        (KeySelectionAttachment)key.attachment()
                        );
            }

        }
    }

    public void receiveMessages(final Timer timer, ReadableByteChannel in, KeySelectionAttachment attachment)
            throws IOException
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
            // FIXME - decode message.
            attachment.clear();
        }
    }

    @Override
    public List<String> nodes() {
        return nodes;
    }

    @Override
    protected WritableByteChannel getOutputStream(String id) {
        return writableByteChannels.get(id);
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
