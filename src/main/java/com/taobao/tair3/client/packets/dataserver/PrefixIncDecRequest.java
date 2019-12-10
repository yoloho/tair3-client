package com.taobao.tair3.client.packets.dataserver;

import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.Counter;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;

public class PrefixIncDecRequest extends AbstractRequestPacket {
    private short namespace = 0;
    private byte[] pkey = null;
    private Map<byte[], Counter> skvs = null;
    public PrefixIncDecRequest(short ns, byte[] pkey, Map<byte[], Counter> skvs) {
        this.namespace = ns;
        this.pkey = pkey;
        this.skvs = skvs;
    }
    public short getNamespace() {
        return this.namespace;
    }
    public static PrefixIncDecRequest build(short ns, byte[] pkey, Map<byte[], Counter> skv) {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (skv == null || skv.size() == 0) {
            throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
        }
        for (Map.Entry<byte[], Counter> entry : skv.entrySet()) {
            byte[] skey = entry.getKey();
            if (skey == null || (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
                throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
            }
            if (entry.getValue() == null) {
                throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
            }
        }
        PrefixIncDecRequest request = new PrefixIncDecRequest(ns, pkey, skv);
        return request;
    }
    @Override
    public void encodeTo(ChannelBuffer buffer) {
        buffer.writeByte(0);
        buffer.writeShort(namespace);
        
        encodeDataMeta(buffer);
        encodeDataEntry(buffer, PREFIX_KEY_TYPE, pkey);
        
        buffer.writeInt(skvs.size());
        for (Map.Entry<byte[], Counter> e : skvs.entrySet()) {
            byte[] skey = e.getKey();
            Counter counter = e.getValue();
            encodeKeyOrValue(buffer, pkey, skey);
            
            counter.setExpire(TairUtil.getDuration(counter.getExpire()));
            buffer.writeLong(counter.getValue());
            buffer.writeLong(counter.getInitValue());
            buffer.writeLong(counter.getExpire());
        }
    }

    public int size() {
        int s = 1;
        s += 2;
        s += METADATA_SIZE + sizeOfDataEntry(PREFIX_KEY_TYPE, pkey);
        s += 4; // key count
        for (Map.Entry<byte[], Counter> e : skvs.entrySet()) {
            s += keyOrValueEncodedSize(pkey, e.getKey());
            s += 8; // value
            s += 8; // init value
            s += 8; // expire
        }
        return s;
    }
}
