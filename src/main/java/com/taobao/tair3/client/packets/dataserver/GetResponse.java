/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 */
package com.taobao.tair3.client.packets.dataserver;

import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.buffer.ChannelBuffer;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.packets.AbstractResponsePacket;

public class GetResponse extends AbstractResponsePacket {
    
    protected List<Result<byte[]>> datas = null;
    protected List<Result<byte[]>> proxyDatas = null;
    public List<Result<byte[]>> getEntrires() {
        return datas;
    }
    
    @Override
    public void decodeFrom(ChannelBuffer buff) {
        resultCode = buff.readInt(); // rc
        int count = buff.readInt(); // value count
        
        datas = new ArrayList<Result<byte[]>>(count);
        for (int i = 0; i < count; i++) {
            Result<byte[]> r = new Result<byte[]>();
            decodeKey(buff, r);
            byte[] value = decodeValue(buff);
            if (r.isCounter()) {
                byte[] rowCount = new byte[8];
                System.arraycopy(value, 2, rowCount, 0, rowCount.length);
                r.setResult(rowCount);
            } else {
                r.setResult(value);
            }
            datas.add(r);
        }
        return ;
    }    
    
    
    public boolean hasConfigVersion() {
        return true;
    }
}
