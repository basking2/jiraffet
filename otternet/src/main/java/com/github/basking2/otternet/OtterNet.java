package com.github.basking2.otternet;

import com.github.basking2.sdsai.net.TcpPool;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.nio.channels.SocketChannel;

public class OtterNet {
    final TcpPool tcpPool;

    public OtterNet() throws IOException {
        this.tcpPool = new TcpPool(
                "0.0.0.0:18079",
                new IdTranslator(),
                new DataHandlerProvider()
        );
    }

    public static final void main(final String[] argv) {

    }

    public static class IdTranslator implements TcpPool.IdTranslator {

        private String schema = "otternet";

        @Override
        public SocketAddress translate(String id) {
            final URI uri = URI.create(id);

            if (!uri.getScheme().equals(schema)) {
                throw new IllegalArgumentException("Scheme must be \"" + schema +"\".");
            }

            return new InetSocketAddress(uri.getHost(), uri.getPort());
        }
    }

    public static class DataHandlerProvider implements TcpPool.DataHandlerProvider {

        @Override
        public TcpPool.DataHandler getDataHandler(String id) {
            return new DataHandler();
        }
    }

    public static class DataHandler implements TcpPool.DataHandler {

        @Override
        public void handleOpen(String id) throws IOException {

        }

        @Override
        public void handleData(String id, SocketChannel chan) throws IOException {

        }

        @Override
        public void handleClose(String id) throws IOException {

        }
    }
}