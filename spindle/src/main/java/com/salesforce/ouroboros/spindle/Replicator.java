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

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.BatchHandler;
import com.lmax.disruptor.Consumer;
import com.lmax.disruptor.ConsumerBarrier;
import com.lmax.disruptor.RingBuffer;

/**
 * A replicator of event streams
 * 
 * @author hhildebrand
 * 
 */
public final class Replicator implements CommunicationsHandler {
    private class ReplicatedState {
        final EventHeader   header;

        final long          offset;
        final AtomicInteger payloadRemaining;
        final FileChannel   segment;

        public ReplicatedState(long offset, EventHeader header,
                               FileChannel segment) {
            this.offset = offset;
            this.header = header;
            this.segment = segment;
            payloadRemaining = new AtomicInteger(header.size());
        }

        public void seekToPayload() throws IOException {
            header.seekToPayload(offset, segment);
        }

        public boolean writeHeader() throws IOException {
            return header.write(segment);
        }

        public boolean writePayload(SocketChannel channel) throws IOException {
            int remaining = payloadRemaining.get();
            long position = header.size() - remaining;
            int written = (int) segment.transferTo(position, remaining, channel);
            if (written == 0) {
                return true;
            }
            payloadRemaining.set(remaining - written);
            return false;
        }
    }

    public enum State {
        WAITING, WRITE_HEADER, WRITE_PAYLOAD;
    }

    private static final Logger                         log             = LoggerFactory.getLogger(Replicator.class);
    private final Bundle                                bundle;
    private final ConsumerBarrier<EventEntry>           consumerBarrier;
    private final Executor                              executor;
    private final AtomicReference<SocketChannelHandler> handler         = new AtomicReference<SocketChannelHandler>();
    private final AtomicReference<ReplicatedState>      replicatedState = new AtomicReference<Replicator.ReplicatedState>();
    private final AtomicBoolean                         running         = new AtomicBoolean();
    private final AtomicLong                            sequence        = new AtomicLong(
                                                                                         RingBuffer.INITIAL_CURSOR_VALUE);
    private final AtomicReference<State>                state           = new AtomicReference<State>(
                                                                                                     State.WAITING);

    /**
     * Construct a batch consumer that will automatically track the progress by
     * updating its sequence when the
     * {@link BatchHandler#onAvailable(AbstractEntry)} method returns.
     * 
     * @param consumerBarrier
     *            on which it is waiting.
     */
    public Replicator(final Bundle bundle,
                      final ConsumerBarrier<EventEntry> consumerBarrier,
                      final Executor executor) {
        this.bundle = bundle;
        this.consumerBarrier = consumerBarrier;
        this.executor = executor;
    }

    @Override
    public void closing(SocketChannel channel) {
        // TODO Auto-generated method stub
    }

    /**
     * Get the {@link ConsumerBarrier} the {@link Consumer} is waiting on.
     * 
     * @return the barrier this {@link Consumer} is using.
     */
    public ConsumerBarrier<EventEntry> getConsumerBarrier() {
        return consumerBarrier;
    }

    public State getState() {
        return state.get();
    }

    public void halt() {
        if (running.compareAndSet(true, false)) {
            consumerBarrier.alert();
        }
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              final SocketChannelHandler handler) {
        this.handler.set(handler);
    }

    @Override
    public void handleRead(SocketChannel channel) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleWrite(SocketChannel channel) {
        switch (state.get()) {
            case WRITE_HEADER: {
                try {
                    if (replicatedState.get().writeHeader()) {

                    }
                } catch (IOException e) {
                    ReplicatedState rs = replicatedState.get();
                    log.error(String.format("Unable to replicate header for event: %s from: %s",
                                            rs.offset, rs.segment), e);
                }
                break;
            }
            case WRITE_PAYLOAD: {
                try {
                    if (replicatedState.get().writePayload(channel)) {
                        state.set(State.WAITING);
                        executor.execute(new Runnable() {
                            public void run() {
                                processNext();
                            }
                        });
                    }
                } catch (IOException e) {
                    ReplicatedState rs = replicatedState.get();
                    log.error(String.format("Unable to replicate payload for event: %s from: %s",
                                            rs.offset, rs.segment), e);
                }
                break;
            }
            default:
                log.error("Illegal write state: " + state);
        }
    }

    private void processNext() {
        if (!running.get()) {
            return;
        }
        long nextSequence = sequence.get() + 1;
        try {
            try {
                consumerBarrier.waitFor(nextSequence);
            } catch (InterruptedException e) {
                return;
            }
            EventEntry entry = consumerBarrier.getEntry(nextSequence);
            sequence.set(entry.getSequence());
            replicate(entry);
        } catch (final AlertException ex) {
            // Wake up from blocking wait
        }
    }

    private void replicate(EventEntry entry) {
        EventHeader header = entry.getHeader();
        ReplicatedState rs = new ReplicatedState(entry.getOffset(), header,
                                                 bundle.segmentFor(header));
        replicatedState.set(rs);
        try {
            rs.seekToPayload();
        } catch (IOException e) {
            log.error(String.format("Unable to seek to event payload on: %s, offset: %s",
                                    rs.segment, rs.offset));
        }
        state.set(State.WRITE_HEADER);
        handler.get().selectForWrite();
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        executor.execute(new Runnable() {
            @Override
            public void run() {
                processNext();
            }
        });
    }
}
