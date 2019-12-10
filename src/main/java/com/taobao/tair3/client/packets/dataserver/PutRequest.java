/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairConstant.MetaFlag;
import com.taobao.tair3.client.util.TairUtil;

public class PutRequest extends AbstractRequestPacket {
    private static final MetaFlag[] EMPTY_FLAG = new MetaFlag[0];
    protected short namespace;
    protected short version;
    protected long expired;
    protected byte[] pkey;
    protected byte[] skey;
    protected MetaFlag[] keyFlag = EMPTY_FLAG;
    protected byte[] val;
    protected MetaFlag[] valueFlag = EMPTY_FLAG;

    public PutRequest(short ns, byte[] pkey, byte[] skey, byte[] val,  short version, long expired) {
        this.namespace = ns;
        this.version = version;
        this.expired = expired;
        this.pkey = pkey;
        this.skey = skey;
        this.val = val;
    }
    @Override
    public void encodeTo(ChannelBuffer buffer) {
        buffer.writeByte((byte) 0); //1
        buffer.writeShort(namespace); //2
        buffer.writeShort(version); //2 
        buffer.writeLong(TairUtil.getDuration(expired)); //8
        //using static buffer
        encodeKeyOrValue(buffer, pkey, skey, keyFlag);
        encodeKeyOrValue(buffer, val, valueFlag);
    }
    
    public int size() {
        int size = 1 + 2;
        size += 2; // version
        size += 8; // expire
        size += keyOrValueEncodedSize(pkey, skey);
        size += keyOrValueEncodedSize(val);
        return size;
    }
    
    public void setKeyFlag(MetaFlag[] keyFlag) {
        this.keyFlag = keyFlag;
    }
    
    public void setValueFlag(MetaFlag[] valueFlag) {
        this.valueFlag = valueFlag;
    }

    public static PutRequest build(short ns, byte[] pkey, byte[] skey, byte[] value, TairOption opt) {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE || (skey != null && ((skey.length + pkey.length + PREFIX_KEY_TYPE.length)> TairConstant.MAX_KEY_SIZE))) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (value == null || value.length > TairConstant.MAX_VALUE_SIZE) {
            throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
        }
        //we must create the instance
        PutRequest request = new PutRequest(ns, pkey, skey, value, opt.getVersion(), opt.getExpire());
        return request;
    }
    public short getNamespace() {
        return this.namespace;
    }
}
