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
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

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

    public EventHeader(int size, int magic, long tag, int crc32) {
        this(ByteBuffer.allocate(HEADER_BYTE_SIZE));
        initialize(size, magic, tag, crc32);
    }

    /**
     * @return the CRC32 value of the payload
     */
    public int getCrc32() {
        return bytes.getInt(CRC_OFFSET);
    }

    /**
     * @return the magic value of the header
     */
    public int getMagic() {
        return bytes.getInt(MAGIC_OFFSET);
    }

    /**
     * @return the value the header is tagged with
     */
    public long getTag() {
        return bytes.getLong(TAG_OFFSET);
    }

    /**
     * Rewind the byte content of the receiver
     */
    public void rewind() {
        bytes.rewind();
    }

    /**
     * @return the size of the payload
     */
    public int size() {
        return bytes.getInt(SIZE_OFFSET) - HEADER_BYTE_SIZE;
    }

    /**
     * Write the byte contents of the receiver on the channel
     * 
     * @param channel
     *            - the channel to write the contents of the receiver
     * @return true if all the bytes of the receiver have been written to the
     *         channel, false if bytes are still remaining
     * @throws IOException
     *             - if problems occur during write
     */
    public boolean write(WritableByteChannel channel) throws IOException {
        channel.write(bytes);
        return !bytes.hasRemaining();
    }

    protected void initialize(int size, int magic, long tag, int crc32) {
        bytes.putInt(size + HEADER_BYTE_SIZE).putInt(magic).putLong(tag).putInt(crc32);
    }
}