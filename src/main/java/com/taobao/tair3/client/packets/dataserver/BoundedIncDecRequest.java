package com.taobao.tair3.client.packets.dataserver;


import org.jboss.netty.buffer.ChannelBuffer;


import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.util.TairConstant;

public class BoundedIncDecRequest extends IncDecRequest {
    protected long lowBound = Long.MIN_VALUE;
    protected long upperBound = Long.MAX_VALUE;
    
    public BoundedIncDecRequest(short ns, byte[] pkey, byte[] skey, long count, long initValue, long expireTime, long lowBound, long upperBound) {
        super(ns, pkey, skey, count, initValue, expireTime);
        this.lowBound = lowBound;
        this.upperBound = upperBound;
    }

    @Override
    public void encodeTo(ChannelBuffer buffer) {
        super.encodeTo(buffer);
        buffer.writeLong(lowBound);
        buffer.writeLong(upperBound);
    }
    @Override
    public int size() {
        return super.size() + 8 + 8;
    }
    
    public static BoundedIncDecRequest build(short ns, byte[] pkey, byte[] skey, long value, long initValue, long lowBound, long upperBound, TairOption opt) throws IllegalArgumentException {
        if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (skey != null && (pkey.length + skey.length + PREFIX_KEY_TYPE.length > TairConstant.MAX_KEY_SIZE)) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (lowBound > upperBound) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        BoundedIncDecRequest request = new BoundedIncDecRequest(ns, pkey, skey, value, initValue, opt.getExpire(), lowBound, upperBound);
        return request;
    }		
}
