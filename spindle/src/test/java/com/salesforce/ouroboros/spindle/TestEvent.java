package com.salesforce.ouroboros.spindle;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;

import org.junit.Test;

public class TestEvent {
    @Test
    public void testOffsets() {
        byte[] src = new byte[23];
        Arrays.fill(src, (byte) 7);
        ByteBuffer payload = ByteBuffer.wrap(src);
        Event event = new Event(777, 666L, payload);
        assertEquals(src.length, event.size());
        assertEquals(666L, event.getTag());
        assertEquals(777, event.getMagic());
        assertEquals(Event.crc32(src), event.getCrc32());
        assertTrue(event.validate());
    }

    @Test
    public void testReadWrite() throws Exception {
        int magic = 666;
        long tag = 777L;
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
