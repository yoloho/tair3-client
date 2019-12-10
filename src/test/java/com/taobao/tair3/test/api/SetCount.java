package com.taobao.tair3.test.api;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairException;
public class SetCount extends TestBase {
    @Test
    public void simpleSetCount() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        try {
            int value = 1;
            int defaultValue = 1;
            Result<Void> rd = tair.invalidByProxy(ns, key, null);
            assertEquals(true , rd.getCode().equals(ResultCode.OK) || rd.getCode().equals(ResultCode.NOTEXISTS));
            
            Result<Void> rs = tair.setCount(ns, key, defaultValue, null);
            assertEquals(ResultCode.OK, rs.getCode());
            
            Result<byte[]> rg = tair.get(ns, key, null);
            assertEquals(ResultCode.OK, rg.getCode());
            assertEquals(true, rg.isCounter());
            
            Result<Long> i = tair.incr(ns, key, value, 0, null);
            assertEquals(ResultCode.OK, i.getCode());
            assertEquals((value + defaultValue), i.getResult().intValue());
            
            Result<Long> d = tair.decr(ns, key, value, 0, null);
            assertEquals(ResultCode.OK, d.getCode());
            assertEquals((defaultValue), d.getResult().intValue());
            
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        }
    }
    
    @Test
    public void minMaxValue() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> rs = tair.setCount(ns, key, Long.MAX_VALUE, opt);
            assertEquals(ResultCode.OK, rs.getCode());
            
            Result<byte[]> rg = tair.get(ns, key, opt);
            assertEquals(ResultCode.OK, rg.getCode());
            assertEquals(true, rg.isCounter());
            assertEquals(Long.MAX_VALUE, rg.getCounter());
            
            rs = tair.setCount(ns, key, Long.MIN_VALUE, opt);
            assertEquals(ResultCode.OK, rs.getCode());
            
            rg = tair.get(ns, key, opt);
            assertEquals(ResultCode.OK, rg.getCode());
            assertEquals(true, rg.isCounter());
            assertEquals(Long.MIN_VALUE, rg.getCounter());
            
            Result<Long> rd = tair.decr(ns, key, 1, 0, opt);
            assertEquals(ResultCode.OK, rd.getCode());
            // overflow
            assertEquals(Long.MAX_VALUE, rd.getResult().longValue());
            
            rg = tair.get(ns, key, opt);
            assertEquals(ResultCode.OK, rg.getCode());
            assertEquals(true, rg.isCounter());
            assertEquals(Long.MAX_VALUE, rg.getCounter());
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        }
    }
}
