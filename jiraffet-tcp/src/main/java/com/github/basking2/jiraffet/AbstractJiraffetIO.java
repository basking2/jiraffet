package com.github.basking2.jiraffet;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.github.basking2.jiraffet.messages.AppendEntriesRequest;
import com.github.basking2.jiraffet.messages.AppendEntriesResponse;
import com.github.basking2.jiraffet.messages.Message;
import com.github.basking2.jiraffet.messages.RequestVoteRequest;
import com.github.basking2.jiraffet.messages.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A partial implementation of {@link JiraffetIO} that knows how to encode and decode messages.
 */
public abstract class AbstractJiraffetIO implements JiraffetIO {

    private final static Logger LOG = LoggerFactory.getLogger(AbstractJiraffetIO.class);

    protected JiraffetProtocol jiraffetProtocol;

    public AbstractJiraffetIO(final JiraffetProtocol jiraffetProtocol) {
        this.jiraffetProtocol = jiraffetProtocol;
    }

    public AbstractJiraffetIO() {
        this(new JiraffetProtocol());
    }

    @Override
    public void requestVotes(RequestVoteRequest req) throws JiraffetIOException {
        final ByteBuffer bb = jiraffetProtocol.marshal(req);

        bb.position(0);

        for (final String node : nodes()) {
            final WritableByteChannel out = getOutputStream(node);
            write(node, "send RequestVoteRequest", bb.slice(), out);
        }

    }

    @Override
    public void requestVotes(String candidateId, RequestVoteResponse req) throws JiraffetIOException {

        // Type and term number.
        final ByteBuffer bb = jiraffetProtocol.marshal(req);

        bb.position(0);
        write(candidateId, "send RequestVoteResponse", bb, getOutputStream(candidateId));
    }

    @Override
    public void appendEntries(String id, AppendEntriesRequest req) throws JiraffetIOException {

        final ByteBuffer bb = jiraffetProtocol.marshal(req);

        bb.position(0);
        write(id, "send AppendEntriesRequest", bb, getOutputStream(id));
    }

    @Override
    public void appendEntries(String id, AppendEntriesResponse resp) throws JiraffetIOException {

        final ByteBuffer bb = jiraffetProtocol.marshal(resp);

        bb.position(0);
        write(id, "send AppendEntriesResponse", bb, getOutputStream(id));
    }

    @Override
    public int nodeCount() {
        return nodes().size();
    }

    @Override
    public abstract List<Message> getMessages(long timeout, TimeUnit timeunit) throws JiraffetIOException, TimeoutException, InterruptedException;

    @Override
    public abstract List<String> nodes();

    /**
     * Given an ID, convert it to a connected, writable byte channel.
     *
     * @param id
     * @return
     * @throws JiraffetIOException
     */
    protected abstract WritableByteChannel getOutputStream(String id) throws JiraffetIOException;

    /**
     * Handle cases where writing fails.
     *
     * @param out The output stream we failed to write to.
     * @param e The precise exception given.
     * @throws JiraffetIOException If this should be fatal to the node.
     */
    protected abstract void handleException(String nodeId, String action, WritableByteChannel out, IOException e) throws JiraffetIOException;

    /**
     * Set position to 0 and write from {@link ByteBuffer#position()} to {@link ByteBuffer#limit()}.
     *
     * @param bb Byte buffer to write.
     * @param chan The channel to write to. This may be null.
     * @throws IOException On any exception.
     */
    public void write(final String nodeId, final String action, final ByteBuffer bb, WritableByteChannel chan) throws JiraffetIOException {
        if (chan == null) {
            return;
        }

        try {
            while (bb.position() < bb.limit()) {
                chan.write(bb);
            }
        }
        catch (final IOException e) {
            handleException(nodeId, action, chan, e);
        }
    }
}
