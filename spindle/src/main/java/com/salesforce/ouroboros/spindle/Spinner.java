package com.salesforce.ouroboros.spindle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;

/**
 * An output sink for a channel.
 * 
 * @author hhildebrand
 * 
 */
public class Spinner implements CommunicationsHandler {
    private enum State {
        INITIALIZED, ACCEPTED, READ_HEADER, APPEND;
    }

    private static final Logger log    = LoggerFactory.getLogger(Spinner.class);

    private ByteBuffer          header = ByteBuffer.allocateDirect(EventHeader.HEADER_BYTE_SIZE);
    private State               state  = State.INITIALIZED;
    private FileChannel         stream;
    private long                remaining;

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        assert state == State.INITIALIZED;
        state = State.ACCEPTED;
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleRead(SocketChannel channel, SocketChannelHandler handler) {
        switch (state) {
            case ACCEPTED: {
                header.reset();
                state = State.READ_HEADER;
                try {
                    readHeader(channel);
                } catch (IOException e) {
                    log.error("Exception during header read", e);
                }
            }
            case READ_HEADER: {
                try {
                    readHeader(channel);
                } catch (IOException e) {
                    log.error("Exception during header read", e);
                }
            }
            case APPEND: {
                try {
                    append(channel);
                } catch (IOException e) {
                    log.error("Exception during append", e);
                }
            }
            default: {
                log.error("Invalid read state: " + state);
            }
        }
        handler.selectForRead();
    }

    private void append(SocketChannel channel) throws IOException {
        remaining -= stream.transferFrom(channel, 0, remaining);
        if (remaining == 0) {
            remaining = -1L;
            stream = null;
            state = State.ACCEPTED;
        }
    }

    private void readHeader(SocketChannel channel) throws IOException {
        channel.read(header);
        if (!header.hasRemaining()) {
            writeHeader();
        }
    }

    private void writeHeader() throws IOException {
        state = State.APPEND;
        remaining = new EventHeader(header).size();
        stream.write(header);
    }

    @Override
    public void handleWrite(SocketChannel channel, SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closing(SocketChannel channel) {
    }

}
