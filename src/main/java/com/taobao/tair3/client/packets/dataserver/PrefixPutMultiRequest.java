package com.taobao.tair3.client.packets.dataserver;

import java.util.Iterator;
import java.util.Map;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.packets.AbstractRequestPacket;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairUtil;
import com.taobao.tair3.client.util.TairConstant.MetaFlag;

public class PrefixPutMultiRequest extends AbstractRequestPacket {
    protected short namespace;
    protected byte[] pkey = null;
    // common values
    protected Map<byte[], Pair<byte[], RequestOption>> values = null;
    // counters
    protected Map<byte[], Pair<byte[], RequestOption>> counters = null;

    public PrefixPutMultiRequest(short ns, byte[] pkey,
            Map<byte[], Pair<byte[], RequestOption>> values,
            Map<byte[], Pair<byte[], RequestOption>> counters) {
        this.namespace = ns;
        this.values = values;
        this.counters = counters;
        this.pkey = pkey;
    }

    @Override
    public void encodeTo(ChannelBuffer out) {
        out.writeByte((byte) 0); // 1
        out.writeShort(namespace); // 2

        encodeDataMeta(out);
        encodeDataEntry(out, PREFIX_KEY_TYPE, pkey);
        int kvSize = 0;
        if (values != null) {
            kvSize += values.size();
        }
        if (counters != null) {
            kvSize += counters.size();
        }
        out.writeInt(kvSize);
        if (values != null) {
            for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : values
                    .entrySet()) {
                byte[] skey = entry.getKey();
                Pair<byte[], RequestOption> value = entry.getValue();
                if (skey == null || value == null || value.isAvaliable() == false) {
                    throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
                }
                // key
                encodeDataMeta(out, value.second().getVersion(), TairUtil.getDuration(value.second().getExpire()));
                encodeKeyOrValueWithoutMeta(out, pkey, skey);
                // value
                encodeKeyOrValue(out, value.first());
            }
        }
        if (counters != null) {
            for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : counters
                    .entrySet()) {
                byte[] skey = entry.getKey();
                Pair<byte[], RequestOption> value = entry.getValue();
                if (skey == null || value == null || value.isAvaliable() == false) {
                    throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
                }
                // key
                encodeDataMeta(out, value.second().getVersion(), TairUtil.getDuration(value.second().getExpire()));
                encodeKeyOrValueWithoutMeta(out, pkey, skey);
                // value
                encodeKeyOrValue(out, value.first(), MetaFlag.ADD_COUNT);
            }
        }
    }

    public int size() {
        int s = 1 + 2;
        s += keyOrValueEncodedSize(pkey) + PREFIX_KEY_TYPE.length;
        s += 4; //kv count
        if (values != null) {
            Iterator<Map.Entry<byte[], Pair<byte[], RequestOption>>> i = values
                    .entrySet().iterator();
            while (i.hasNext()) {
                Map.Entry<byte[], Pair<byte[], RequestOption>> entry = i.next();
                byte[] skey = entry.getKey();
                Pair<byte[], RequestOption> value = entry.getValue();
                s += keyOrValueEncodedSize(pkey, skey);
                s += keyOrValueEncodedSize(value.first());
            }
        }
        if (counters != null) {
            Iterator<Map.Entry<byte[], Pair<byte[], RequestOption>>> i = counters
                    .entrySet().iterator();
            while (i.hasNext()) {
                Map.Entry<byte[], Pair<byte[], RequestOption>> entry = i.next();
                byte[] skey = entry.getKey();
                Pair<byte[], RequestOption> value = entry.getValue();
                s += keyOrValueEncodedSize(pkey, skey);
                s += keyOrValueEncodedSize(value.first());
            }
        }
        return s;
    }

    public static PrefixPutMultiRequest build(short ns, byte[] pkey,
            Map<byte[], Pair<byte[], RequestOption>> keyValuePairs,
            Map<byte[], Pair<byte[], RequestOption>> keyCounterPairs) {
        if (ns < 0 || ns >= TairConstant.NAMESPACE_MAX) {
            throw new IllegalArgumentException(TairConstant.NS_NOT_AVAILABLE);
        }
        if (pkey == null || pkey.length > TairConstant.MAX_KEY_SIZE) {
            throw new IllegalArgumentException(TairConstant.KEY_NOT_AVAILABLE);
        }
        if (keyValuePairs == null && keyCounterPairs == null) {
            throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
        }
        if (keyValuePairs != null) {
            if (keyValuePairs.size() == 0) {
                throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
            }
            for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : keyValuePairs
                    .entrySet()) {
                byte[] skey = entry.getKey();
                if (skey == null
                        || (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
                    throw new IllegalArgumentException(
                            TairConstant.KEY_NOT_AVAILABLE);
                }
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                            TairConstant.VALUE_NOT_AVAILABLE);
                }
                if (entry.getValue().first() == null
                        || entry.getValue().second() == null) {
                    throw new IllegalArgumentException(
                            TairConstant.VALUE_NOT_AVAILABLE);
                }
            }
        }
        if (keyCounterPairs != null) {
            if (keyCounterPairs.size() == 0) {
                throw new IllegalArgumentException(TairConstant.VALUE_NOT_AVAILABLE);
            }
            for (Map.Entry<byte[], Pair<byte[], RequestOption>> entry : keyCounterPairs
                    .entrySet()) {
                byte[] skey = entry.getKey();
                if (skey == null
                        || (pkey.length + skey.length + PREFIX_KEY_TYPE.length) > TairConstant.MAX_KEY_SIZE) {
                    throw new IllegalArgumentException(
                            TairConstant.KEY_NOT_AVAILABLE);
                }
                if (entry.getValue() == null) {
                    throw new IllegalArgumentException(
                            TairConstant.VALUE_NOT_AVAILABLE);
                }
                if (entry.getValue().first() == null
                        || entry.getValue().second() == null) {
                    throw new IllegalArgumentException(
                            TairConstant.VALUE_NOT_AVAILABLE);
                }
            }
        }
        PrefixPutMultiRequest request = new PrefixPutMultiRequest(ns, pkey,
                keyValuePairs, keyCounterPairs);
        return request;
    }
    public short getNamespace() {
        return this.namespace;
    }
}
