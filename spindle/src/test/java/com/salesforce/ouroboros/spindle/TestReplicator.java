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
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.Executors;

import org.junit.Test;

import com.hellblazer.pinkie.SocketChannelHandler;
import com.hellblazer.pinkie.SocketOptions;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.ConsumerBarrier;
import com.salesforce.ouroboros.spindle.Replicator.State;

/**
 * 
 * @author hhildebrand
 * 
 */
public class TestReplicator {

    @Test
    public void testEventReplication() throws Exception {
        File tmpFile = File.createTempFile("event-replication", ".tst");
        tmpFile.deleteOnExit();

        RandomAccessFile ra = new RandomAccessFile(tmpFile, "rw");
        FileChannel segment = ra.getChannel();

        int magic = 666;
        UUID tag = UUID.randomUUID();
        final byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        EventHeader event = new EventHeader(payload.length, magic, tag,
                                            Event.crc32(payload));
        event.rewind();
        event.write(segment);
        segment.write(payloadBuffer);
        segment.force(false);

        EventEntry entry = new EventEntry();
        entry.setHeader(event);
        entry.setOffset(0);

        Bundle bundle = mock(Bundle.class);
        @SuppressWarnings("unchecked")
        ConsumerBarrier<EventEntry> consumerBarrier = mock(ConsumerBarrier.class);
        SocketChannelHandler handler = mock(SocketChannelHandler.class);

        when(bundle.segmentFor(isA(EventHeader.class))).thenReturn(segment,
                                                                   segment);
        when(consumerBarrier.waitFor(0)).thenReturn(0L).thenThrow(AlertException.ALERT_EXCEPTION);
        when(consumerBarrier.getEntry(0)).thenReturn(entry);

        final Replicator replicator = new Replicator(
                                                     bundle,
                                                     consumerBarrier,
                                                     Executors.newSingleThreadExecutor());
        assertEquals(State.WAITING, replicator.getState());
        SocketOptions options = new SocketOptions();
        options.setTimeout(100);
        ServerSocketChannel server = ServerSocketChannel.open();
        server.configureBlocking(true);
        server.socket().bind(new InetSocketAddress(0));
        final SocketChannel outbound = SocketChannel.open();
        options.configure(outbound.socket());
        outbound.configureBlocking(true);
        outbound.connect(server.socket().getLocalSocketAddress());
        final SocketChannel inbound = server.accept();
        options.configure(inbound.socket());
        inbound.configureBlocking(true);

        assertTrue(inbound.isConnected());
        outbound.configureBlocking(true);
        inbound.configureBlocking(true);
        final ByteBuffer replicated = ByteBuffer.allocate(EventHeader.HEADER_BYTE_SIZE
                                                          + payload.length);
        Thread inboundRead = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    int read = 0;
                    for (read += inbound.read(replicated); read < EventHeader.HEADER_BYTE_SIZE
                                                                  + payload.length; read += inbound.read(replicated)) {
                        System.out.println("read: " + read);
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            return;
                        }
                    }
                    System.out.println("read: " + read);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }, "Inbound read thread");
        inboundRead.start();
        replicator.handleConnect(outbound, handler);
        Util.waitFor("Never achieved WRITE_HEADER state", new Util.Condition() {

            @Override
            public boolean value() {
                return State.WRITE_HEADER == replicator.getState();
            }
        }, 1000L, 100L);
        replicator.halt();
        Util.waitFor("Never achieved WRITE_PAYLOAD state",
                     new Util.Condition() {
                         @Override
                         public boolean value() {
                             replicator.handleWrite(outbound);
                             return State.WRITE_PAYLOAD == replicator.getState();
                         }
                     }, 1000L, 100L);
        Util.waitFor("Never achieved WAITING state", new Util.Condition() {
            @Override
            public boolean value() {
                replicator.handleWrite(outbound);
                return State.WAITING == replicator.getState();
            }
        }, 1000L, 100L);
        inboundRead.join(4000);
        replicated.flip();
        assertTrue(replicated.hasRemaining());

        Event replicatedEvent = new Event(replicated);
        assertEquals(event.size(), replicatedEvent.size());
        assertEquals(event.getMagic(), replicatedEvent.getMagic());
        assertEquals(event.getCrc32(), replicatedEvent.getCrc32());
        assertTrue(replicatedEvent.validate());
    }
}
