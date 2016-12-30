package com.github.basking2.jiraffet;

import com.github.basking2.jiraffet.messages.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * How Jiraffet messages are encoded and decoded.
 *
 * Implementors are encouraged to wrap this class in their own implementation to add features like
 * message encryption and authentication.
 */
public class JiraffetProtocol {
    public static final int REQUEST_VOTE_REQUEST = 1;
    public static final int REQUEST_VOTE_RESPONSE = 2;
    public static final int APPEND_ENTRIES_REQUEST = 3;
    public static final int APPEND_ENTRIES_RESPONSE = 4;

    public ByteBuffer marshal(final Message message) {
        if (message instanceof RequestVoteResponse) {
            return marshal((RequestVoteResponse)message);
        }
        else if (message instanceof RequestVoteRequest) {
            return marshal((RequestVoteRequest)message);
        }
        else if (message instanceof AppendEntriesResponse) {
            return marshal((AppendEntriesResponse) message);
        }
        else if (message instanceof AppendEntriesRequest) {
            return marshal((AppendEntriesRequest) message);
        }
        else {
            throw new IllegalArgumentException("Unsupported class: "+message.getClass());
        }
    }
    
    private ByteBuffer marshal(final RequestVoteResponse message) {
        final int len = 16;

        // Type and term number.
        final ByteBuffer bb = ByteBuffer.allocate(len);

        bb.putInt(len);
        bb.putInt(REQUEST_VOTE_RESPONSE);
        bb.putInt(message.isVoteGranted()? 1 : 0);
        bb.putInt(message.getTerm());
        bb.position(0);

        return bb;
    }

    private ByteBuffer marshal(final RequestVoteRequest message) {
        int len = 24;

        len += message.getCandidateId().getBytes().length;

        final ByteBuffer bb = ByteBuffer.allocate(len);

        bb.putInt(len);
        bb.putInt(REQUEST_VOTE_REQUEST);
        bb.putInt(message.getLastLogIndex());
        bb.putInt(message.getLastLogTerm());
        bb.putInt(message.getTerm());
        bb.putInt(message.getCandidateId().getBytes().length);
        bb.put(message.getCandidateId().getBytes());
        bb.position(0);

        return bb;
    }

    private ByteBuffer marshal(final AppendEntriesResponse message) {
        int len = 20;

        len += message.getFrom().getBytes().length;

        final ByteBuffer bb = ByteBuffer.allocate(len);

        bb.putInt(len);
        bb.putInt(APPEND_ENTRIES_RESPONSE);
        bb.putInt(message.getNextCommitIndex());
        bb.putInt(message.isSuccess() ? 1 : 0);
        bb.putInt(message.getFrom().getBytes().length);
        bb.put(message.getFrom().getBytes());

        bb.position(0);

        return bb;
    }

    private ByteBuffer marshal(final AppendEntriesRequest message) {
        // type, len, leaderCommit, logIndex, logTerm, term = 6*4 = 24 bytes.
        int len = 32;

        // add a number of byte buffers.
        for (final byte[] msg: message.getEntries()) {
            len += 4;
            len += msg.length;
        }

        // Add the byte length.
        len += message.getLeaderId().getBytes().length;

        final ByteBuffer bb = ByteBuffer.allocate(len);

        bb.putInt(len);
        bb.putInt(APPEND_ENTRIES_REQUEST);
        bb.putInt(message.getLeaderCommit());
        bb.putInt(message.getPrevLogIndex());
        bb.putInt(message.getPrevLogTerm());
        bb.putInt(message.getTerm());
        bb.putInt(message.getLeaderId().getBytes().length);
        bb.put(message.getLeaderId().getBytes());

        bb.putInt(message.getEntries().size());

        for (final byte[] msg: message.getEntries()) {
            bb.putInt(msg.length);
            bb.put(msg);
        }

        bb.position(0);
        return bb;
    }

    /**
     * @param header The first 8 bytes of every message contain the same data. The message length, the message type.
     *               This is that data.
     * @param bb The rest of the message data.
     * @return The parsed message.
     * @throws IllegalArgumentException On an unknown message type.
     */
    public Message unmarshal(final ByteBuffer header, final ByteBuffer bb) {
        final int type = header.getInt(4);
        bb.position(0);
        switch(type) {
            case REQUEST_VOTE_REQUEST:
                return unmarshalRequestVoteRequest(bb);
            case REQUEST_VOTE_RESPONSE:
                return unmarshalRequestVoteResponse(bb);
            case APPEND_ENTRIES_REQUEST:
                return unmarshalAppendEntriesRequest(bb);
            case APPEND_ENTRIES_RESPONSE:
                return unmarshalAppendEntriesResponse(bb);
            default:
                throw new IllegalArgumentException("No message type "+type);
        }
    }

    private Message unmarshalRequestVoteResponse(final ByteBuffer bb) {
        final RequestVoteResponse requestVoteResponse = new RequestVoteResponse();

        requestVoteResponse.setVoteGranted(bb.getInt() != 0);
        requestVoteResponse.setTerm(bb.getInt());

        return requestVoteResponse;
    }

    private Message unmarshalRequestVoteRequest(final ByteBuffer bb) {
        final RequestVoteRequest requestVoteRequest = new RequestVoteRequest();

        requestVoteRequest.setLastLogIndex(bb.getInt());
        requestVoteRequest.setLastLogTerm(bb.getInt());
        requestVoteRequest.setTerm(bb.getInt());

        byte[] candidateId = new byte[bb.getInt()];
        bb.get(candidateId);
        requestVoteRequest.setCandidateId(new String(candidateId));

        return requestVoteRequest;
    }

    private Message unmarshalAppendEntriesResponse(final ByteBuffer bb) {
        final AppendEntriesResponse appendEntriesResponse = new AppendEntriesResponse();

        appendEntriesResponse.setNextCommitIndex(bb.getInt());
        appendEntriesResponse.setSuccess(bb.getInt() != 0);
        byte[] from = new byte[bb.getInt()];
        bb.get(from);
        appendEntriesResponse.setFrom(new String(from));

        return appendEntriesResponse;
    }

    private Message unmarshalAppendEntriesRequest(final ByteBuffer bb) {
        final AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest();

        appendEntriesRequest.setLeaderCommit(bb.getInt());
        appendEntriesRequest.setPrevLogIndex(bb.getInt());
        appendEntriesRequest.setPrevLogTerm(bb.getInt());
        appendEntriesRequest.setTerm(bb.getInt());

        final byte[] leaderId = new byte[bb.getInt()];
        bb.get(leaderId);
        appendEntriesRequest.setLeaderId(new String(leaderId));

        final int entriesCount = bb.getInt();
        final List<byte[]> entries = new ArrayList<>(entriesCount);
        for (int i = 0; i < entriesCount; ++i) {
            byte[] entry = new byte[bb.getInt()];
            bb.get(entry);
            entries.add(entry);
        }

        appendEntriesRequest.setEntries(entries);

        return appendEntriesRequest;
    }

    public int getType(final ByteBuffer header) {
        return header.getInt(4);
    }

    public int getLen(final ByteBuffer header) {
        return header.getInt(0);
    }
}
