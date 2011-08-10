package com.salesforce.ouroboros.spindle;

import java.nio.channels.FileChannel;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.EntryFactory;

/**
 * The ring buffer entry representing an event
 * 
 * @author hhildebrand
 * 
 */
public class EventEntry extends AbstractEntry {
    private long                                 offset;
    private FileChannel                          channel;

    public final static EntryFactory<EventEntry> ENTRY_FACTORY = new EntryFactory<EventEntry>() {
                                                                   @Override
                                                                   public EventEntry create() {
                                                                       return new EventEntry();
                                                                   }
                                                               };

    public FileChannel getChannel() {
        return channel;
    }

    public long getOffset() {
        return offset;
    }

    public void setChannel(FileChannel channel) {
        this.channel = channel;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
