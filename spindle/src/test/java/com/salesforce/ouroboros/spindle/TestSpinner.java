/**
 * Copyright (c) 2011, salesforce.com, inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided
 * that the following conditions are met:
 *
 *    Redistributions of source code must retain the above copyright notice, this list of conditions and the
 *    following disclaimer.
 *
 *    Redistributions in binary form must reproduce the above copyright notice, this list of conditions and
 *    the following disclaimer in the documentation and/or other materials provided with the distribution.
 *
 *    Neither the name of salesforce.com, inc. nor the names of its contributors may be used to endorse or
 *    promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
 * ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
 * TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
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
        ByteBuffer writtenPayload = event.getPayload();
        for (byte b : payload) {
            assertEquals(b, writtenPayload.get());
        }

        verify(handler, new Times(3)).selectForRead();
        verify(bundle).segmentFor(isA(EventHeader.class));
        verifyNoMoreInteractions(handler, bundle);
    }
}
