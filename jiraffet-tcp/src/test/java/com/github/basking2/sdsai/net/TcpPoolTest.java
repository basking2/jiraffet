package com.github.basking2.sdsai.net;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 */
public class TcpPoolTest {

    @Test
    public void testSend() throws Exception {

        final List<ByteBuffer> msgs = new ArrayList<>();
        String addr1 = "tcp://localhost:9865";
        TcpPool pool1 = new TcpPool(addr1, new TcpPool.DefaultIdTranslator(), id -> new ReceiveToBuffer(msgs));

        String addr2 = "tcp://localhost:9866";
        TcpPool pool2 = new TcpPool(addr2, new TcpPool.DefaultIdTranslator(), id -> new ReceiveToBuffer(msgs));

        TcpPool.UserKeyAttachment a = pool1.connect(addr2);
        String text = "Hullo, world.";
        a.write(ByteBuffer.allocate(4).putInt(0, text.getBytes().length));
        a.write(ByteBuffer.wrap(text.getBytes()));

        pool1.runOnceNow();
        pool2.runOnceNow();

        // Ownership changes.
        pool1.runOnceNow();
        pool2.runOnceNow();

        pool1.runOnceNow();
        pool2.runOnceNow();

        pool1.close();
        pool2.close();

        assertEquals(text, new String(msgs.get(0).array()));
    }

    public static class ReceiveToBuffer implements TcpPool.DataHandler {
        private ByteBuffer header;
        private ByteBuffer body;
        private List<ByteBuffer> msgs;

        public ReceiveToBuffer(final List<ByteBuffer> msgs) {
            this.msgs = msgs;
            this.header = ByteBuffer.allocate(4);
            this.body = null;
        }

        @Override
        public void handleOpen(String id) throws IOException {
        }

        @Override
        public void handleData(String id, SocketChannel chan) throws IOException {
            chan.read(header);

            if (header.position() == header.limit()) {
                if (body == null) {
                    body = ByteBuffer.allocate(header.getInt(0));
                    msgs.add(body);
                }
                chan.read(body);
            }
        }

        @Override
        public void handleClose(String id) throws IOException {

        }
    };
}
