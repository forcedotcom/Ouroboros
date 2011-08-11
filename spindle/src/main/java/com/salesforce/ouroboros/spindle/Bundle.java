package com.salesforce.ouroboros.spindle;

import java.nio.channels.FileChannel;

/**
 * 
 * @author hhildebrand
 * 
 */
public interface Bundle {
    FileChannel segmentFor(EventHeader header);
}
