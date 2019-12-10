package com.taobao.tair3.test.api;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairException;
public class PrefixSetCount extends TestBase {
    @Test
    public void simpleSetCount() {
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        byte[] skey = UUID.randomUUID().toString().getBytes();
        try {
            int value = 1;
            int defaultValue = 1;
            Result<Void> rd = tair.prefixInvalidByProxy(ns, pkey, skey, null);
            assertEquals(true , rd.getCode().equals(ResultCode.OK) || rd.getCode().equals(ResultCode.NOTEXISTS));
            
            Result<Void> rs = tair.prefixSetCount(ns, pkey, skey, defaultValue, opt);
            assertEquals(ResultCode.OK, rs.getCode());
            
            Result<byte[]> rg = tair.prefixGet(ns, pkey, skey, opt);
            assertEquals(ResultCode.OK, rg.getCode());
            assertEquals(true, rg.isCounter());
            assertEquals(defaultValue, rg.getCounter());
            
            Result<Long> i = tair.prefixIncr(ns, pkey, skey, value, defaultValue, opt);
            assertEquals(ResultCode.OK, i.getCode());
            assertEquals((value + defaultValue), i.getResult().intValue());
            
            Result<Long> d = tair.prefixDecr(ns, pkey, skey, value, defaultValue, opt);
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
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        byte[] skey = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> rs = tair.prefixSetCount(ns, pkey, skey, Long.MAX_VALUE, opt);
            assertEquals(ResultCode.OK, rs.getCode());
            
            Result<byte[]> rg = tair.prefixGet(ns, pkey, skey, opt);
            assertEquals(ResultCode.OK, rg.getCode());
            assertEquals(true, rg.isCounter());
            assertEquals(Long.MAX_VALUE, rg.getCounter());
            
            rs = tair.prefixSetCount(ns, pkey, skey, Long.MIN_VALUE, opt);
            assertEquals(ResultCode.OK, rs.getCode());
            
            rg = tair.prefixGet(ns, pkey, skey, opt);
            assertEquals(ResultCode.OK, rg.getCode());
            assertEquals(true, rg.isCounter());
            assertEquals(Long.MIN_VALUE, rg.getCounter());
            
            Result<Long> rd = tair.prefixDecr(ns, pkey, skey, 1, 0, opt);
            assertEquals(ResultCode.OK, rd.getCode());
            // overflow
            assertEquals(Long.MAX_VALUE, rd.getResult().longValue());
            
            rg = tair.prefixGet(ns, pkey, skey, opt);
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
