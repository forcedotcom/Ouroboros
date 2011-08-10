package com.salesforce.ouroboros.spindle;

import java.nio.ByteBuffer;
import java.util.Arrays;

import junit.framework.TestCase;

public class TestEvent extends TestCase {
    public void testOffsets() {
        byte[] src = new byte[23];
        Arrays.fill(src, (byte) 7);
        ByteBuffer payload = ByteBuffer.wrap(src);
        Event event = new Event(666L, 777, payload);
        assertEquals(src.length, event.size());
        assertEquals(666L, event.getTag());
        assertEquals(777, event.getMagic());
        assertEquals(Event.crc32(src), event.getCrc32());
        assertTrue(event.validate());
    }
}
