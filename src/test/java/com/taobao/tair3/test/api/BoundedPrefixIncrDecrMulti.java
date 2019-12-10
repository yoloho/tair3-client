package com.taobao.tair3.test.api;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.TairClient.Counter;
import com.taobao.tair3.client.error.TairException;

public class BoundedPrefixIncrDecrMulti  extends TestBase {
 
    protected static int lowBound = -100;
    protected static int upperBound = 100;

    @Test
    public void normalIncr() {
        //1. create a counter
        int keyCount = 10;
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        List<byte[]> skeys = this.generateKeys(keyCount);
        Map<byte[], Counter> skv = new HashMap<byte[], Counter>();
        for (byte[] skey : skeys) {
            skv.put(skey, new Counter(0,0,0));
        }
        try {
            tair.prefixInvalidMultiByProxy(ns, pkey, skeys, opt);
            ResultMap<byte[], Result<Long>> pi = tair.prefixIncrMulti(ns, pkey, skv, opt);
            assertEquals(ResultCode.OK, pi.getCode());
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        }
    }
}
