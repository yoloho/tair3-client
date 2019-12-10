package com.taobao.tair3.client.packets.invalidserver;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class HideByProxyMultiRequest extends AbstractRequestPacket {
    protected short namespace;
    //1) pkey != null && keys != null prefixDeleteMulti
    protected byte[] pkey = null;
    protected List<byte[]> skeys = null;
    protected String group;
    protected int isSync = 1;
     
    
    public HideByProxyMultiRequest(short ns, byte[] pkey, List<byte[]> skeys, String group) {
        this.namespace = ns;
        this.pkey = pkey;
        this.skeys = skeys;
        this.group = group;
        if (!this.group.endsWith("\0")) {
            this.group += "\0";
        }
    }
    
    public static HideByProxyMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys, String groupName) throws IllegalArgumentException {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (skeys == null) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        for (byte[] skey : skeys) {
            if (skey == null || (skey.length + pkey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
                throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
            }
        }
        
        HideByProxyMultiRequest request = new HideByProxyMultiRequest(ns, pkey, skeys, groupName);
        return request;
    }

    @Override
    public void encodeTo(ChannelBuffer out) {
        if (pkey == null) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        out.writeByte((byte)0); // 1
        out.writeShort(namespace); // 2
        out.writeInt(skeys.size()); // key count
        for (byte[] skey : skeys) {
            if (skey == null) {
                throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
            }
            encodeKeyOrValue(out, pkey, skey);
        }
        out.writeInt(group.getBytes().length); // group name length
        out.writeBytes(group.getBytes()); // group name
        out.writeInt(isSync); // sync
    }

    public int size() {
        int s = 1;
        s += 2;
        s += 4; // key count
        for (byte[] skey : skeys) {
            s += keyOrValueEncodedSize(pkey, skey);
        }
        s += 4;
        s += group.getBytes().length;
        s += 4;
        return s;
    }

    @Override
    public short getNamespace() {
        return namespace;
    }
}
