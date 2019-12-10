package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class RangeRequest extends AbstractRequestPacket {
    // Considering the key is string in most cases so we can 
    // define the min and max by ascii encoding value.
    private static final byte[] MIN_KEY = new byte[] {0};
    private static final byte[] MAX_KEY = new byte[] {0x7f};
    protected short type;
    protected short ns;
    protected byte[] pkey = null;
    protected byte[] startKey = null;
    protected byte[] endKey = null;
    
    protected int offset;
    protected int maxCount;
    
    /**
     * @param ns
     * @param pkey
     * @param startKey
     * @param endKey
     * @param offset
     * @param maxCount
     * @param type see {@link TairConstant#RANGE_ALL}, {@link TairConstant#RANGE_ALL_REVERSE}
     */
    public RangeRequest(short ns, byte[] pkey, byte[] startKey, byte[] endKey, int offset, int maxCount, short type) {
        this.ns = ns;
        this.pkey = pkey;
        switch (type) {
            case TairConstant.RANGE_ALL:
            case TairConstant.RANGE_KEY_ONLY:
            case TairConstant.RANGE_VALUE_ONLY:
                if (startKey == null) {
                    startKey = MIN_KEY;
                }
                if (endKey == null) {
                    endKey = MAX_KEY;
                }
                break;
                
            case TairConstant.RANGE_ALL_REVERSE:
            case TairConstant.RANGE_KEY_ONLY_REVERSE:
            case TairConstant.RANGE_VALUE_ONLY_REVERSE:
                if (startKey == null) {
                    startKey = MAX_KEY;
                }
                if (endKey == null) {
                    endKey = MIN_KEY;
                }
                break;

            default:
                throw new RuntimeException("type should be one of {TairConstant.RANGE_*}");
        }
        this.startKey = startKey;
        this.endKey = endKey;
        this.offset = offset;
        this.maxCount = maxCount;
        this.type = type;
    }
     @Override
    public void encodeTo(ChannelBuffer buffer) {
        if (startKey == null || endKey == null) {
            throw new IllegalArgumentException();
        }
        buffer.writeByte((byte)0);
        buffer.writeShort(type);
        buffer.writeShort(ns);
        buffer.writeInt(offset);
        buffer.writeInt(maxCount);
        encodeKeyOrValue(buffer, pkey, startKey);
        encodeKeyOrValue(buffer, pkey, endKey);
     }
    public int size() { 
         int s = 1;
         s += 2;
         s += 2; // ns
         s += 4; // offset
         s += 4; // count
         s += keyOrValueEncodedSize(pkey, startKey);
         s += keyOrValueEncodedSize(pkey, endKey);
         return s;
     }
    public static RangeRequest build(short ns, byte[] pkey, byte[] start, byte[] end, int offset, int maxCount, short type, TairOption opt) throws IllegalArgumentException {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (offset < 0 || maxCount < 0) {
            throw new IllegalArgumentException(TairConstant.OPTION_NOT_AVAILABLE);
        }
        RangeRequest request = new RangeRequest(ns, pkey, start, end, offset, maxCount, type);
        return request;
    }
    public short getNamespace() {
        return this.ns;
    }
}
