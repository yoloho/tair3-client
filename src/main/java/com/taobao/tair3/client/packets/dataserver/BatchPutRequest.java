package com.taobao.tair3.client.packets.dataserver;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;

public class BatchPutRequest extends AbstractRequestPacket {
    protected short namespace;
    protected short version;
    protected long expired;
    protected List<byte[]> keySet = new ArrayList<byte[]>();
    protected List<byte[]> valSet = new ArrayList<byte[]>();
    public void setNamespace(short namespace) {
        this.namespace = namespace;
    }
    public short getNamespace() {
        return this.namespace;
    }
    public void setVersion(short version) {
        this.version = version;
    }
    public void setExpired(long expired) {
        this.expired = expired;
    }
    
    void addKey(byte[] key) {
        this.keySet.add(key);
    }
    void addVal(byte[] val) {
        this.valSet.add(val);
    }
    public void encodeTo(ChannelBuffer buffer) {
        if (keySet.size() != valSet.size()) {
            throw new IllegalArgumentException("key and val should be fully paired");
        }
        buffer.writeByte((byte) 0); // 1
        buffer.writeShort(namespace); // 2
        buffer.writeShort(version); // 2
        buffer.writeLong(expired); // 8

        buffer.writeInt(keySet.size()); // key count
        encodeKeyOrValue(buffer, keySet);
        
        buffer.writeInt(valSet.size()); // value count
        encodeKeyOrValue(buffer, valSet);
    }

    public int size() {
        int s = 1;
        s += 2;
        s += 2;
        s += 8;
        
        s += 4;
        s += keyOrValueEncodedSize(keySet);
        s += 4;
        s += keyOrValueEncodedSize(valSet);
        return s;
    }
    public List<byte[]> getKeySet()  {
        return keySet;
    }
    public static BatchPutRequest build(short ns, byte[] key, byte[] val, TairOption opt) throws IllegalArgumentException {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        BatchPutRequest req = new BatchPutRequest();
        req.setExpired(opt.getExpire());
        req.setVersion(opt.getVersion());
        req.setNamespace(ns);
        req.addKey(key);
        req.addVal(val);
        return req;
    }
    public static BatchPutRequest build(short ns, Map<ByteArray, byte[]> kv, TairOption opt) throws IllegalArgumentException {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        BatchPutRequest req = new BatchPutRequest();
        req.setExpired(opt.getExpire());
        req.setVersion(opt.getVersion());
        req.setNamespace(ns);
        Iterator<Map.Entry<ByteArray, byte[]>> it = kv.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ByteArray, byte[]> entry = it.next();
            byte[] key = entry.getKey().getBytes();
            byte[] val = entry.getValue();
            req.addKey(key);
            req.addVal(val);
        }
        return req;
    }
}