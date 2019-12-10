package com.taobao.tair3.client.packets;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.util.TairConstant;



public abstract class AbstractResponsePacket extends AbstractPacket {
    protected int resultCode = -1;
    
    /**
     * Decode the meta segment
     * 
     * @param buff
     * @param r null for just skip the meta segment
     */
    protected <T> void decodeMeta(ChannelBuffer buff, Result<T> r) {
        buff.readByte(); // isMerged
        buff.readInt(); // area
        buff.readShort(); // serverFlag	
        buff.readShort(); // magic code
        buff.readShort(); // check sum
        buff.readShort(); // key size
        Short version = buff.readShort();
        buff.readInt();	// pad size	
        buff.readInt(); // value size
        Byte flag = buff.readByte();
        long cdate = buff.readLong(); // cdate
        long mdate = buff.readLong(); // mdate
        long edate = buff.readLong(); // edate
        if (r != null) {
            r.setVersion(version);
            r.setFlag(flag);
            r.setExpire(edate);
            r.setModifyTime(mdate);
            r.setCreateTime(cdate);
        }
    }
    
    /**
     * Decode and skip a meta segment
     * 
     * @param buff
     */
    protected void decodeMeta(ChannelBuffer buff) {
        decodeMeta(buff, null);
    }
    
    protected <T> void decodeKey(ChannelBuffer buff, Result<T> r) {
        decodeMeta(buff, r);
        int len = buff.readInt();
        int size = (len & TairConstant.MASK_KEY_LENGTH);
        short prefixSize = (short)(len >> TairConstant.PREFIX_KEY_OFFSET);

        //with prefix key
        if (prefixSize > 0) {
            size -= PREFIX_KEY_TYPE.length;
            prefixSize -= PREFIX_KEY_TYPE.length;
            // prefix type sign (2 bytes)
            buff.skipBytes(PREFIX_KEY_TYPE.length);
        }
        byte[] keyBytes = new byte[size];
        buff.readBytes(keyBytes);
        r.setKey(keyBytes, prefixSize);
    }
    
    /**
     * @param buff
     * @return null for empty value (len = 0)
     */
    protected byte[] decodeValue(ChannelBuffer buff) {
        decodeMeta(buff);
        int len = buff.readInt();
        if (len > 0) {
            byte[] data = new byte[len];
            buff.readBytes(data);
            return data;
        }
        return null;
    }

    public byte[] getKey() {
        return null;
    }
    
    public void setCode(int code) {
        resultCode = code;
    }
    
    public int getCode () {
        return resultCode;
    }
    
    public int decodeConfigVersionFrom(ChannelBuffer bb) {
        return bb.readInt();
    }
    public int decodeResultCodeFrom(ChannelBuffer bb) {
        int r = bb.getInt(4);
        return r;
    }
    
}
