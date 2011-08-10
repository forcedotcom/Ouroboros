package com.salesforce.ouroboros.spindle;

/**
 * 
 * @author hhildebrand
 * 
 */
public interface Bundle {
    Channel channelFor(String subscription);
}
