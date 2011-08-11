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
    public enum State {
        INITIALIZED, ACCEPTED, READ_HEADER, APPEND;
    }

    private static final Logger log   = LoggerFactory.getLogger(Spinner.class);

    private final ByteBuffer    header;
    private State               state = State.INITIALIZED;
    private FileChannel         segment;
    private long                remaining;
    private long                position;
    private final Bundle        bundle;

    public Spinner(Bundle bundle) {
        this.bundle = bundle;
        header = ByteBuffer.allocate(EventHeader.HEADER_BYTE_SIZE);
        header.mark();
    }

    @Override
    public void closing(SocketChannel channel) {
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
        assert state == State.INITIALIZED;
        state = State.ACCEPTED;
        handler.selectForRead();
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
                readHeader(channel);
                break;
            }
            case READ_HEADER: {
                readHeader(channel);
                break;
            }
            case APPEND: {
                append(channel);
                break;
            }
            default: {
                log.error("Invalid read state: " + state);
            }
        }
        handler.selectForRead();
    }

    @Override
    public void handleWrite(SocketChannel channel, SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "Spinner [state=" + state + ", header=" + header + ", segment="
               + segment + ", remaining=" + remaining + ", position="
               + position + "]";
    }

    public State getState() {
        return state;
    }

    private void append(SocketChannel channel) {
        long written;
        try {
            written = segment.transferFrom(channel, position, remaining);
        } catch (IOException e) {
            log.error("Exception during append", e);
            return;
        }
        position += written;
        remaining -= written;
        if (remaining == 0) {
            try {
                segment.position(position);
            } catch (IOException e) {
                log.error("Exception positioning segment after append", e);
            }
            segment = null;
            state = State.ACCEPTED;
        }
    }

    private void readHeader(SocketChannel channel) {
        try {
            channel.read(header);
        } catch (IOException e) {
            log.error("Exception during header read", e);
            return;
        }
        if (!header.hasRemaining()) {
            EventHeader eventHeader = new EventHeader(header);
            segment = bundle.segmentFor(eventHeader);
            try {
                position = segment.size();
            } catch (IOException e) {
                log.error("Exception during header read", e);
                return;
            }
            writeHeader();
            remaining = eventHeader.size();
            append(channel);
        }
    }

    private void writeHeader() {
        header.rewind();
        try {
            segment.write(header);
        } catch (IOException e) {
            log.error("Exception during header read", e);
            return;
        }
        try {
            position = segment.position();
        } catch (IOException e) {
            log.error("Exception during reading of segment position", e);
            return;
        }
        state = State.APPEND;
    }

}
