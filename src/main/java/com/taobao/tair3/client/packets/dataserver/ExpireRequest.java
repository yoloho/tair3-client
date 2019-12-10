package com.taobao.tair3.client.packets.dataserver;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;

public class ExpireRequest extends AbstractRequestPacket {
    protected short namespace = 0;
    protected byte[] key = null;
    protected long expire;
    public ExpireRequest(short namespace, byte[] key, long expired) {
        this.namespace = namespace;
        this.key = key;
        this.expire = expired;
    }

    @Override
    public void encodeTo(ChannelBuffer buffer) {
        buffer.writeByte((byte)0); // 1
        buffer.writeShort(namespace); // 2
        buffer.writeLong(TairUtil.getDuration(expire)); // expre
        encodeKeyOrValue(buffer, key);
    }
    
    public int size() {
        int s = 1;
        s += 2;
        s += 8;
        s += keyOrValueEncodedSize(key);
        return s;
    }
    
    public short getNamespace() {
        return this.namespace;
    }
    public static ExpireRequest build(short ns, byte[] key, TairOption opt) {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (key == null || key.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (opt.getExpire() < 0) {
            throw new IllegalArgumentException(TairConstant.EXPIRE_TIME_NOT_AVAILABLE);
        }
        //is available ?
        long expriedTime = TairUtil.getDuration(opt.getExpire());
        ExpireRequest req = new ExpireRequest(ns, key, expriedTime);
        return req;
    }
}
