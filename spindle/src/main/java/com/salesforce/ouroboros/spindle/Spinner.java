package com.salesforce.ouroboros.spindle;

import java.nio.channels.SocketChannel;

import com.hellblazer.pinkie.CommunicationsHandler;
import com.hellblazer.pinkie.SocketChannelHandler;
import com.lmax.disruptor.ProducerBarrier;

/**
 * An output sink for a channel.
 * 
 * @author hhildebrand
 * 
 */
public class Spinner implements CommunicationsHandler {
    private final ProducerBarrier<EventEntry> sink;
    private final Bundle                      channels;

    public Spinner(Bundle channels, ProducerBarrier<EventEntry> sink) {
        this.channels = channels;
        this.sink = sink;
    }

    @Override
    public void handleAccept(SocketChannel channel, SocketChannelHandler handler) {
    }

    @Override
    public void handleConnect(SocketChannel channel,
                              SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void handleRead(SocketChannel channel, SocketChannelHandler handler) {
        // TODO Auto-generated method stub

    }

    @Override
    public void handleWrite(SocketChannel channel, SocketChannelHandler handler) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void closing(SocketChannel channel) {
    }

}
