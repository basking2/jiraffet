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


/**
 * A partial implementation of {@link JiraffetIO} that knows how to encode and decode messages.
 */
public abstract class AbstractJiraffetIO implements JiraffetIO {

    protected JiraffetProtocol jiraffetProtocol;

    public AbstractJiraffetIO(final JiraffetProtocol jiraffetProtocol) {
        this.jiraffetProtocol = jiraffetProtocol;
    }

    public AbstractJiraffetIO() {
        this(new JiraffetProtocol());
    }

    @Override
    public void requestVotes(RequestVoteRequest req) throws IOException {
        final ByteBuffer bb = jiraffetProtocol.marshal(req);

        bb.position(0);

        for (final String node : nodes()) {
            write(bb.slice(), getOutputStream(node));
        }
        
    }

    @Override
    public void requestVotes(String candidateId, RequestVoteResponse req) throws IOException {

        // Type and term number.
        final ByteBuffer bb = jiraffetProtocol.marshal(req);

        bb.position(0);
        write(bb, getOutputStream(candidateId));
    }

    @Override
    public void appendEntries(String id, AppendEntriesRequest req) throws IOException {

        final ByteBuffer bb = jiraffetProtocol.marshal(req);

        bb.position(0);
        write(bb, getOutputStream(id));
    }

    @Override
    public void appendEntries(String id, AppendEntriesResponse resp) throws IOException {

        final ByteBuffer bb = jiraffetProtocol.marshal(resp);

        bb.position(0);
        write(bb, getOutputStream(id));
    }

    @Override
    public int nodeCount() {
        return nodes().size();
    }

    @Override
    public abstract List<Message> getMessages(long timeout, TimeUnit timeunit) throws IOException, TimeoutException, InterruptedException;

    @Override
    public abstract List<String> nodes();
    protected abstract WritableByteChannel getOutputStream(String id) throws IOException;

    /**
     * Set position to 0 and write from {@link ByteBuffer#position()} to {@link ByteBuffer#limit()}.
     *
     * @param bb Byte buffer to write.
     * @param chan The channel to write to.
     * @throws IOException On any exception.
     */
    public void write(final ByteBuffer bb, WritableByteChannel chan) throws IOException {
        while (bb.position() < bb.limit()) {
            chan.write(bb);
        }
    }
}
