package com.salesforce.ouroboros.spindle;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
/**
 * 
 * @author hhildebrand
 * 
 */
public class TestEventHeader {
    @Test
    public void testOffsets() {
        EventHeader header = new EventHeader(25, 777, 666L, 23456);
        assertEquals(25, header.size());
        assertEquals(666L, header.getTag());
        assertEquals(777, header.getMagic());
        assertEquals(23456, header.getCrc32());
    }
}
