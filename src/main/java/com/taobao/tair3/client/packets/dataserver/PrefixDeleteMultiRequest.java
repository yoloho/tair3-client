package com.taobao.tair3.client.packets.dataserver;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class PrefixDeleteMultiRequest extends AbstractRequestPacket {
    protected short namespace;
    //1) pkey != null && keys != null prefixDeleteMulti
    protected byte[] pkey = null;
    protected List<byte[]> skeys = null;

    public PrefixDeleteMultiRequest(short ns, byte[] pkey, List<byte[]> skeys) {
        this.namespace = ns;
        this.pkey = pkey;
        this.skeys = skeys;
    }

    public short getNamespace() {
        return this.namespace;
    }
    @Override
    public void encodeTo(ChannelBuffer out) {
        out.writeByte((byte)0); // 1
        out.writeShort(namespace); // 2
        out.writeInt(skeys.size()); // key count
        for (byte[] skey : skeys) {
            encodeKeyOrValue(out, pkey, skey);
        }
    }

    public int size() {
        int s = 1;
        s += 2;
        s += 4; // key count
        for (byte[] skey : skeys) {
            s += keyOrValueEncodedSize(pkey, skey);
        }
        return s;
    }

    public static PrefixDeleteMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys) {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (skeys == null || skeys.size() == 0) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        for (byte[] key : skeys) {
            if (key == null || (pkey.length + key.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
                throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
            }
        }
        PrefixDeleteMultiRequest request = new PrefixDeleteMultiRequest(ns, pkey, skeys);
        return request;
    }
}
