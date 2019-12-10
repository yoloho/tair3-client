package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;

public class IncDecRequest extends AbstractRequestPacket {
    protected short namespace = 0;
    protected long count = 1;
    protected long initValue = 0;
    protected long expireTime = 0;
    // only two cases:
    // pkey != null && skey == null
    // pkey == null && skey != null
    protected byte[] pkey = null;
    protected byte[] skey = null;
    //protected short prefixSize = 0;
    
    public short getNamespace() {
        return this.namespace;
    }
    public IncDecRequest(short ns, byte[] pkey, byte[] skey, long count, long initValue, long expireTime) {
        this.namespace = ns;
        this.pkey = pkey;
        this.skey = skey;
        this.count = count;
        this.initValue = initValue;
        this.expireTime = expireTime;
    }
    
    @Override
    public void encodeTo(ChannelBuffer buffer) {
        buffer.writeByte((byte)0); // 1
        buffer.writeShort(namespace); // 2
        buffer.writeLong(count); // 8
        buffer.writeLong(initValue); //8
        buffer.writeLong(TairUtil.getDuration(expireTime)); // 8
        
        encodeKeyOrValue(buffer, pkey, skey);
    }
    public int size() {
        int s = 1;
        s += 2;
        s += 8; // val
        s += 8; // init
        s += 8; // expire
        s += keyOrValueEncodedSize(pkey, skey);
        return s;
    }
    
    public static IncDecRequest build(short ns, byte[] pkey, byte[] skey, long value, long initValue, TairOption opt) throws IllegalArgumentException {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length)> TairConstant.MAX_KEY_SIZE)) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        IncDecRequest request = new IncDecRequest(ns, pkey, skey, value, initValue, opt.getExpire());
        return request;
    }	
}
