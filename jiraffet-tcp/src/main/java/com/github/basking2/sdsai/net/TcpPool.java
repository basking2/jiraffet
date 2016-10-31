package com.github.basking2.sdsai.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.WritableByteChannel;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manage a set of connections to various hosts.
 */
public class TcpPool implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TcpPool.class);
    private IdTranslator idTranslator;
    public static final int ID_LIMIT = 1000;

    private Map<String, WritableByteChannel> writableByteChannels;
    private ServerSocketChannel serverSocketChannel;
    private String nodeId;
    private Selector selector;

    public TcpPool(final String listen) throws IOException {
        this(new DefaultIdTranslator(), listen);
    }

    public TcpPool(final IdTranslator idTranslator, final String listen) throws IOException {
        this.idTranslator = idTranslator;
        this.selector = Selector.open();
        this.writableByteChannels = new HashMap<>();
        this.serverSocketChannel = ServerSocketChannel.open().bind(idTranslator.translate(listen));
        this.serverSocketChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        this.serverSocketChannel.configureBlocking(false);
        this.serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
        this.nodeId = listen;
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

    @FunctionalInterface
    public interface IdTranslator {
        SocketAddress translate(String id);
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

}
