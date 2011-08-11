package com.salesforce.ouroboros.spindle;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Test;
import org.mockito.internal.verification.Times;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.salesforce.ouroboros.spindle.Spinner.State;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestSpinner {

    @Test
    public void testAppend() throws Exception {
        final SocketChannelHandler handler = mock(SocketChannelHandler.class);
        Bundle bundle = mock(Bundle.class);
        File tmpFile = File.createTempFile("append-test", ".dat");
        tmpFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tmpFile);
        final FileChannel writeSegment = fos.getChannel();
        when(bundle.segmentFor(isA(EventHeader.class))).thenReturn(writeSegment);
        final Spinner spinner = new Spinner(bundle);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress(0));
        SocketChannel outbound = SocketChannel.open();
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        inbound.configureBlocking(false);

        spinner.handleAccept(inbound, handler);
        assertEquals(Spinner.State.ACCEPTED, spinner.getState());

        int magic = 666;
        long tag = 777L;
        byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        EventHeader header = new EventHeader(payload.length, magic, tag,
                                             Event.crc32(payload));
        header.rewind();
        header.write(outbound);

        Util.waitFor("Header has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                spinner.handleRead(inbound, handler);
                return spinner.getState() == State.APPEND;
            }
        }, 1000, 100);

        outbound.write(payloadBuffer);

        Util.waitFor("Payload has not been fully read", new Util.Condition() {
            @Override
            public boolean value() {
                spinner.handleRead(inbound, handler);
                return spinner.getState() == State.ACCEPTED;
            }
        }, 1000, 100);

        writeSegment.force(true);
        outbound.close();
        inbound.close();
        server.close();
        writeSegment.close();

        FileInputStream fis = new FileInputStream(tmpFile);
        FileChannel readSegment = fis.getChannel();
        Event event = new Event(readSegment);
        readSegment.close();
        assertTrue(event.validate());
        assertEquals(magic, event.getMagic());
        assertEquals(tag, event.getTag());
        assertEquals(payload.length, event.size());

        verify(handler, new Times(3)).selectForRead();
        verify(bundle).segmentFor(isA(EventHeader.class));
        verifyNoMoreInteractions(handler, bundle);
    }
}
