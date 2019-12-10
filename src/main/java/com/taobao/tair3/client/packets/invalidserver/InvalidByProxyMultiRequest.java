package com.taobao.tair3.client.packets.invalidserver;

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
public class InvalidByProxyMultiRequest extends AbstractRequestPacket {
    protected short namespace;
    //only 2 cases:
    //1) keys != null, invlaid
    //2) pkey != null && keys != null, prefixInvalid
    protected byte[] pkey = null;
    protected List<byte[]> keys = null;
    protected String group = null;
    protected int isSync = 1;


    public InvalidByProxyMultiRequest(short ns, List<byte[]> keys, String group) {
        this.namespace = ns;
        this.keys = keys;
        this.pkey = null;
        this.group = group;
        if (!this.group.endsWith("\0")) {
            this.group += "\0";
        }
    }
    public InvalidByProxyMultiRequest(short ns, byte[] pkey, List<byte[]> skeys, String group) {
        this.namespace = ns;
        this.pkey = pkey;
        this.keys = skeys;
        this.group = group;
        if (!this.group.endsWith("\0")) {
            this.group += "\0";
        }
    }
    
    public static InvalidByProxyMultiRequest build(short ns, List<byte[]> keys, String groupName) throws IllegalArgumentException {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (keys == null ) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        for (byte[] key : keys) {
            if (key == null || key.length > TairConstant.MAX_KEY_SIZE) {
                throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
            }
        }
        InvalidByProxyMultiRequest request = new InvalidByProxyMultiRequest(ns, keys, groupName);
        return request;
    }
    public static InvalidByProxyMultiRequest build(short ns, byte[] pkey, List<byte[]> skeys, String groupName) throws IllegalArgumentException {
        if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
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
        InvalidByProxyMultiRequest request = new InvalidByProxyMultiRequest(ns, pkey, skeys, groupName);
        return request;
    }
    @Override
    public void encodeTo(ChannelBuffer out) {
        out.writeByte((byte)0); // 1
        out.writeShort(namespace); // 2
        out.writeInt(keys.size()); // key count
        for (byte[] key : keys) {
            encodeKeyOrValue(out, pkey, key);
        }
        out.writeInt(group.getBytes().length); // group name length
        out.writeBytes(group.getBytes()); // group name
        out.writeInt(isSync); // sync
    }
    @Override
    public int size() {
        int s = 1;
        s += 2;
        s += 4; // key count
        for (byte[] key : keys) {
            s += keyOrValueEncodedSize(pkey, key);
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
