package com.taobao.tair3.client.packets.invalidserver;

/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */

import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;

public class InvalidByProxyRequest extends AbstractRequestPacket {
    protected short namespace;
    protected byte[] pkey = null;
    protected byte[] skey = null;
    protected List<byte[]> keys = null;
    protected String group;
    protected int isSync = 1;

    public InvalidByProxyRequest(short namespace, byte[] pkey, byte[] skey, String group) {
        this.namespace = namespace;
        this.pkey = pkey;
        this.skey = skey;
        this.group = group;
        if (!this.group.endsWith("\0")) {
            this.group += "\0";
        }
        this.keys = null;
    }
    public InvalidByProxyRequest(short namespace, List<byte[]> keys, String group) {
        this.namespace = namespace;
        this.pkey = null;
        this.skey = null;
        this.group = group;
        if (!this.group.endsWith("\0")) {
            this.group += "\0";
        }
        this.keys = keys;
    }

    @Override
    public void encodeTo(ChannelBuffer out) {
        out.writeByte((byte)0); // 1
        out.writeShort(namespace); // 2
        if (keys == null) {
            out.writeInt(1); // 4, only one key
            encodeKeyOrValue(out, pkey, skey);
        } else {
            out.writeInt(keys.size()); // key count
            encodeKeyOrValue(out, keys);
        }
        out.writeInt(group.getBytes().length);
        out.writeBytes(group.getBytes());
        out.writeInt(isSync);
    }
    
    @Override
    public int size() {
        int s = 1;
        s += 2;
        s += 4;
        if (keys == null) {
            // single
            s += keyOrValueEncodedSize(pkey, skey);
        } else {
            // multi
            keyOrValueEncodedSize(keys);
        }
        s += 4; // group name length
        s += group.getBytes().length;
        s += 4;
        return s;
    }
    
    public static InvalidByProxyRequest build(short ns, byte[] pkey, byte[] skey, String groupName) throws IllegalArgumentException {
        if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (skey != null && ((pkey.length + skey.length + PREFIX_KEY_TYPE.length)> TairConstant.MAX_KEY_SIZE)) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        //if (!groupName.endsWith("\0")) {
        //	groupName += "\0";
        //}
        InvalidByProxyRequest request = new InvalidByProxyRequest(ns, pkey, skey, groupName);
        return request;
    }
    
    public static InvalidByProxyRequest build(short ns, List<byte[]> keys, String groupName) throws IllegalArgumentException {
        if (ns <0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (keys == null) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        for (byte[] key : keys) {
            if (key == null || key.length > TairConstant.MAX_KEY_SIZE) {
                throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
            }
        }
        InvalidByProxyRequest request = new InvalidByProxyRequest(ns, keys, groupName);
        return request;
    }
    @Override
    public short getNamespace() {
        return namespace;
    }
}
