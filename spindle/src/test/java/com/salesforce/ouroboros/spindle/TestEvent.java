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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

/**
 * 
 * @author hhildebrand
 *
 */
public class TestEvent {
    @Test
    public void testOffsets() {
        byte[] src = new byte[23];
        Arrays.fill(src, (byte) 7);
        ByteBuffer payload = ByteBuffer.wrap(src);
        UUID tag = UUID.randomUUID();
        Event event = new Event(777, tag, payload);
        assertEquals(src.length, event.size());
        assertEquals(tag, event.getTag());
        assertEquals(777, event.getMagic());
        assertEquals(Event.crc32(src), event.getCrc32());
        assertTrue(event.validate());
    }

    @Test
    public void testReadWrite() throws Exception {
        int magic = 666;
        UUID tag = UUID.randomUUID();
        byte[] payload = "Give me Slack, or give me Food, or Kill me".getBytes();
        ByteBuffer payloadBuffer = ByteBuffer.wrap(payload);
        Event written = new Event(magic, tag, payloadBuffer);

        File tmpFile = File.createTempFile("read-write", ".tst");
        tmpFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tmpFile);
        FileChannel segment = fos.getChannel();
        written.rewind();
        assertTrue(written.write(segment));
        segment.close();

        assertEquals(magic, written.getMagic());
        assertEquals(tag, written.getTag());
        assertEquals(payload.length, written.size());

        FileInputStream fis = new FileInputStream(tmpFile);
        segment = fis.getChannel();
        Event read = new Event(segment);
        segment.close();
        assertEquals(written.getTag(), read.getTag());
        assertEquals(written.getTag(), read.getTag());
        assertEquals(written.size(), read.size());
    }
}
