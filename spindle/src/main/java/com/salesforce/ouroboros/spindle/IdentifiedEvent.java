package com.salesforce.ouroboros.spindle;

import java.nio.ByteBuffer;

/**
 * An event that has been identified within a channel
 * 
 * @author hhildebrand
 * 
 */
public class IdentifiedEvent extends Event {
    private final long id;

    public IdentifiedEvent(long id, ByteBuffer bytes) {
        super(bytes);
        this.id = id;
    }

    public long getId() {
        return id;
    }
}
