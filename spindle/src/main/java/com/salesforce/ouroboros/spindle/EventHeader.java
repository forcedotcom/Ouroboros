package com.salesforce.ouroboros.spindle;

import java.nio.ByteBuffer;

/**
 * An event header comprised of:
 * 
 * <pre>
 *      4 byte size
 *      4 byte magic
 *      8 byte tag
 *      4 byte CRC32
 * </pre>
 * 
 * @author hhildebrand
 * 
 */
public class EventHeader {

    protected static final int SIZE_OFFSET      = 0;
    protected static final int MAGIC_OFFSET     = SIZE_OFFSET + 4;
    protected static final int TAG_OFFSET       = MAGIC_OFFSET + 4;
    protected static final int CRC_OFFSET       = TAG_OFFSET + 8;
    protected static final int HEADER_BYTE_SIZE = CRC_OFFSET + 4;

    protected final ByteBuffer bytes;

    public EventHeader(ByteBuffer bytes) {
        this.bytes = bytes;
    }

    public int getCrc32() {
        return bytes.getInt(CRC_OFFSET);
    }

    public int getMagic() {
        return bytes.getInt(MAGIC_OFFSET);
    }

    public long getTag() {
        return bytes.getLong(TAG_OFFSET);
    }

    public int size() {
        return bytes.getInt(SIZE_OFFSET) - HEADER_BYTE_SIZE;
    }

}