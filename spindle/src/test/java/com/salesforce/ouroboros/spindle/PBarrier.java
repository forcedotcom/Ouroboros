package com.salesforce.ouroboros.spindle;

import com.lmax.disruptor.ProducerBarrier;
import com.lmax.disruptor.SequenceBatch;

public class PBarrier implements ProducerBarrier<EventEntry> {

    @Override
    public void commit(SequenceBatch sequenceBatch) {
    }

    @Override
    public void commit(EventEntry entry) {
    }

    @Override
    public long getCursor() {
        return 0;
    }

    @Override
    public EventEntry getEntry(long sequence) {
        return null;
    }

    @Override
    public SequenceBatch nextEntries(SequenceBatch sequenceBatch) {
        return null;
    }

    @Override
    public EventEntry nextEntry() {
        return new EventEntry();
    }
}