package com.taobao.tair3.client.packets;
import java.util.Collection;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.rpc.protocol.tair2_3.Packet;
import com.taobao.tair3.client.rpc.protocol.tair2_3.PacketManager;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairConstant.MetaFlag;


public abstract class AbstractPacket implements Packet {
    protected static final short NAMESPACE_MAX = Short.MAX_VALUE;
    
    protected static final int METADATA_SIZE = 48;
    protected static final int KEY_SIZE_PREFIX_SHIFT = 2;
    protected static final byte[] COMMON_META = new byte[METADATA_SIZE];
    //flag
    protected static final byte[] PREFIX_KEY_TYPE = new byte[2];
    protected Object context = null;
    static {
        PREFIX_KEY_TYPE[1] = (byte) ((12 << 1) & 0xFF);
        PREFIX_KEY_TYPE[0] = (byte) (((12 << 1) >> 8) & 0xFF);
    }
    
    /**
     * Write a data into buffer with field of length
     * <pre>
     * -----------------
     * | len | data(s) |
     * -----------------
     * 
     * @param out
     * @param datas
     */
    protected void encodeDataEntry(ChannelBuffer out, byte[]... datas) {
        if (datas.length == 0) {
            throw new IllegalArgumentException();
        }
        int len = 0;
        for (byte[] data : datas) {
            len += data.length;
        }
        out.writeInt(len);
        for (byte[] data : datas) {
            out.writeBytes(data);
        }
    }
    
    protected int sizeOfDataEntry(byte[]... datas) {
        if (datas.length == 0) {
            throw new IllegalArgumentException();
        }
        int len = 0;
        for (byte[] data : datas) {
            len += data.length;
        }
        return len + 4;
    }
    
    /**
     * For list of common keys
     * 
     * @param out
     * @param keyList
     */
    protected void encodeKeyOrValue(ChannelBuffer out, Collection<byte[]> keyList, MetaFlag... flags) {
        if (keyList == null || keyList.size() < 1) {
            throw new IllegalArgumentException();
        }
        for (byte[] key : keyList) {
            encodeDataMeta(out, flags);
            out.writeInt(key.length);
            out.writeBytes(key);
        }
    }
    
    protected void encodeKeyOrValue(ChannelBuffer out, byte[] primaryKey, MetaFlag... flags) {
        encodeKeyOrValue(out, primaryKey, null, flags);
    }
    
    /**
     * Encode key into buffer
     * 
     * @param out
     * @param primaryKey
     * @param subKey if not null it's a subkey with prefix
     */
    protected void encodeKeyOrValue(ChannelBuffer out, byte[] primaryKey, byte[] subKey, MetaFlag... flags) {
        encodeDataMeta(out, flags);
        encodeKeyOrValueWithoutMeta(out, primaryKey, subKey);
    }
    
    protected void encodeKeyOrValueWithoutMeta(ChannelBuffer out, byte[] primaryKey, byte[] subKey) {
        if (primaryKey == null || primaryKey.length == 0) {
            throw new IllegalArgumentException();
        }
        out.writeInt(keyOrValueSize(primaryKey, subKey)); // key size
        if (subKey != null) {
            // it's a prefix type key
            out.writeBytes(PREFIX_KEY_TYPE);
        }
        out.writeBytes(primaryKey);
        if (subKey != null) {
            out.writeBytes(subKey);
        }
    }
    
    protected int keyOrValueEncodedSize(Collection<byte[]> keyList) {
        int s = 0;
        for (byte[] key : keyList) {
            s += keyOrValueSize(key);
            s += METADATA_SIZE; // meta
            s += 4; // key size
        }
        return s;
    }
    
    protected int keyOrValueEncodedSize(byte[] primaryKey) {
        return keyOrValueEncodedSize(primaryKey, null);
    }
    
    protected int keyOrValueEncodedSize(byte[] primaryKey, byte[] subKey) {
        int s = keyOrValueSize(primaryKey, subKey) & TairConstant.MASK_KEY_LENGTH;
        s += METADATA_SIZE; // meta
        s += 4; // key size
        return s;
    }
    
    /**
     * Generate the key size segment
     * 
     * @param pkey
     * @return
     */
    protected int keyOrValueSize(byte[] primaryKey) {
        return keyOrValueSize(primaryKey, null);
    }
    
    /**
     * Generate the key size segment
     * <p>
     * For simple keys there only be `pkey`<br>
     * For prefix based keys there will be `pkey` and `skey`(sub key)<br>
     * 
     * @param primaryKey
     * @param subKey
     * @return
     */
    protected int keyOrValueSize(byte[] primaryKey, byte[] subKey) {
        if (primaryKey == null || primaryKey.length == 0) {
            throw new IllegalArgumentException();
        }
        return keyOrValueSize(primaryKey.length, subKey == null ? 0 : subKey.length);
    }
    
    /**
     * Generate the key size segment
     * <p>
     * For simple keys there only be `pkey`<br>
     * For prefix based keys there will be `pkey` and `skey`(sub key)<br>
     * 
     * @param pkeyLen
     * @param subKeyLen
     * @return
     */
    protected int keyOrValueSize(int pkeyLength, int subKeyLength) {
        int s = pkeyLength;
        if (subKeyLength > 0) {
            s += PREFIX_KEY_TYPE.length;
            s <<= TairConstant.PREFIX_KEY_OFFSET;
            s |= (pkeyLength + subKeyLength + PREFIX_KEY_TYPE.length);
        }
        return s;
    }
    
    /**
     * @param out
     * @param flags
     */
    protected void encodeDataMeta(ChannelBuffer out, MetaFlag... flags) {
        if (flags.length == 0) {
            // empty meta
            out.writeBytes(COMMON_META);
        } else {
            byte[] meta = new byte[METADATA_SIZE];
            int flag = 0;
            for (MetaFlag f : flags) {
                flag |= f.getVal();
            }
            meta[23] = (byte)(flag & 0xff);
            out.writeBytes(meta);
        }
    }

    protected void encodeDataMeta(ChannelBuffer out, short version, long expire, MetaFlag... flags) {
        int flag = 0;
        for (MetaFlag f : flags) {
            flag |= f.getVal();
        }
        out.writeZero(13);
        out.writeShort(version);
        out.writeInt(0);
        out.writeInt(0);
        out.writeByte((byte)(flag & 0xff));
        out.writeLong(0);
        out.writeLong(0);
        out.writeLong(expire);
    }
    public int getPacketCode() {
        return PacketManager.getPacketCode(this.getClass());
    }
    
    public int size() {
        return 0;
    }
    
    public void decodeFrom(ChannelBuffer bb) {
        throw new UnsupportedOperationException("decode not implement " + getClass().getName());
    }
    
    public void encodeTo(ChannelBuffer bb) {
        throw new UnsupportedOperationException("encode not implement " + getClass().getName());
    }

    public Object  getContext() {
        return context;
    }

    public void setContext(Object context) {
        this.context = context;
    } 
    public int decodeResultCodeFrom(ChannelBuffer bb) {
        throw new UnsupportedOperationException("decode resultcode not implement " + getClass().getName());
    }
   
}
