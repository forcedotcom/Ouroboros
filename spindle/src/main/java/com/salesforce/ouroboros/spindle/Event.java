package com.salesforce.ouroboros.spindle;

import java.nio.ByteBuffer;

/**
 * The currency of the channels.
 * 
 * Events are identified by their 64 bit offset within a channel. Each event has
 * a header comprised of:
 * 
 * <pre>
 *      4 byte size
 *      4 byte magic
 *      4 byte CRC32
 * </pre>
 * 
 * The body of the event consists of uninterpreted bytes. The size of the
 * payload is the 4 byte size - 12 (for the header)
 * 
 * @author hhildebrand
 * 
 */
public class Event {
    private static final int HEADER_BYTE_SIZE = 12;
    private static final int SIZE_OFFSET      = 0;
    private static final int MAGIC_OFFSET     = SIZE_OFFSET + 4;
    private static final int CRC_OFFSET       = MAGIC_OFFSET + 4;

    private final ByteBuffer bytes;

    public Event(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public int size() {
        return bytes.getInt(SIZE_OFFSET) - HEADER_BYTE_SIZE;
    }

    public int getMagic() {
        return bytes.getInt(MAGIC_OFFSET);
    }

    public int getCrc32() {
        return bytes.getInt(CRC_OFFSET);
    }

    public ByteBuffer getPayload() {
        bytes.position(HEADER_BYTE_SIZE);
        return bytes.slice().asReadOnlyBuffer();
    }
}
