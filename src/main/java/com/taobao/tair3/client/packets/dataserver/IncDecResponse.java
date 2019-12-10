package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractResponsePacket;

public class IncDecResponse  extends AbstractResponsePacket {
    protected long value = 0;
    
    public long getValue() {
        return value;
    }
    
    @Override
    public void decodeFrom(ChannelBuffer buffer) {
        value = buffer.readLong();
        resultCode = 0;
        return;
    }
  
    public boolean hasConfigVersion() {
        return true;
    }
    @Override
    public int decodeResultCodeFrom(ChannelBuffer buf) {
        return 0;
    }
}
