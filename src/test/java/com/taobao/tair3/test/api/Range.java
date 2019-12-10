package com.taobao.tair3.test.api;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairException;

public class Range extends TestBase {
    @Test
    public void simpelGetRange() {
        int keyCount = 20;
        byte[] pkey =   UUID.randomUUID().toString().getBytes();
        List<byte[]> skeys = this.generateOrderedKeys(UUID.randomUUID().toString().getBytes(), keyCount);
        Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
        for (byte[] key : skeys) {
            kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
        }
        try {
            ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, opt);
            assertEquals(ResultCode.OK, pm.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
            assertEquals(ResultCode.OK, gm.getCode());
            assertEquals(keyCount, pm.size());
            for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            byte[] start = skeys.get(0);
            byte[] end = skeys.get(keyCount - 1);
            
            // [min, max)
            Result<List<Pair<byte[], Result<byte[]>>>> r = tair.getRange(ns, pkey, start, end, 0, keyCount, false, opt);
            assertEquals(ResultCode.OK, r.getCode());
            assertEquals(keyCount - 1, r.getResult().size());
        	for (Pair<byte[], Result<byte[]>> e : r.getResult()) {
        		assertEquals(ResultCode.OK, e.second().getCode());
        	}
            
        	// forward all
            Result<List<Pair<byte[], Result<byte[]>>>> r1 = tair.getRange(ns, pkey, null, null, 0, keyCount, false, opt);
            assertEquals(ResultCode.OK, r1.getCode());
            assertEquals(keyCount, r1.getResult().size());
        	for (Pair<byte[], Result<byte[]>> e : r1.getResult()) {
        		assertEquals(ResultCode.OK, e.second().getCode());
        	}
        	
        	// reverse all
            Result<List<Pair<byte[], Result<byte[]>>>> r2 = tair.getRange(ns, pkey, null, null, 0, keyCount, true, opt);
            assertEquals(ResultCode.OK, r2.getCode());
            assertEquals(keyCount, r2.getResult().size());
            for (Pair<byte[], Result<byte[]>> e : r2.getResult()) {
                assertEquals(ResultCode.OK, e.second().getCode());
            }
            
            // reverse [max, min)
            Result<List<Pair<byte[], Result<byte[]>>>> r3 = tair.getRange(ns, pkey, end, start, 0, keyCount, true, opt);
            assertEquals(ResultCode.OK, r3.getCode());
            assertEquals(keyCount - 1, r3.getResult().size());
            for (Pair<byte[], Result<byte[]>> e : r3.getResult()) {
                assertEquals(ResultCode.OK, e.second().getCode());
            }
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
        } 
    }
    
    @Test
    public void simpelGetRangeKey() {
        int keyCount = 9;
        byte[] pkey =   UUID.randomUUID().toString().getBytes();
        List<byte[]> skeys = this.generateOrderedKeys(UUID.randomUUID().toString().getBytes(), keyCount);
        Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
        for (byte[] key : skeys) {
            kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
        }
        try {
            ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, opt);
            assertEquals(ResultCode.OK, pm.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
            assertEquals(ResultCode.OK, gm.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
         
            byte[] start = skeys.get(0);
            byte[] end = skeys.get(keyCount - 1);
            Result<List<Pair<byte[], Result<byte[]>>>> rall = tair.getRange(ns, pkey, start, end, 0, keyCount, false, opt);
            assertEquals(ResultCode.OK, rall.getCode());
            assertEquals(keyCount - 1, rall.getResult().size());
            assertArrayEquals(start, rall.getResult().get(0).first());
            assertArrayEquals(skeys.get(keyCount - 2), rall.getResult().get(rall.getResult().size() - 1).first());
            
            Result<List<Result<byte[]>>> r = tair.getRangeKey(ns, pkey, start, end, 0, keyCount, false, opt);
            assertEquals(ResultCode.OK, r.getCode());
            assertEquals(keyCount - 1, r.getResult().size());
            
            Result<List<Result<byte[]>>> r1 = tair.getRangeKey(ns, pkey, null, null, 0, keyCount, false, opt);
            assertEquals(ResultCode.OK, r1.getCode());
            assertEquals(keyCount, r1.getResult().size());
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
        } 
    }
    
    @Test
    public void simpelGetRangeValue() {
        int keyCount = 20;
        byte[] pkey =   UUID.randomUUID().toString().getBytes();
        List<byte[]> skeys = this.generateOrderedKeys(UUID.randomUUID().toString().getBytes(), keyCount);
        Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
        for (byte[] key : skeys) {
            kvs.put(key, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), new RequestOption()));
        }
        try {
            TairOption opt = new TairOption(500, (short)0, 0);
            ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, opt);
            assertEquals(ResultCode.OK, pm.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : pm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
            assertEquals(ResultCode.OK, gm.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : gm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
         
            
            byte[] start = skeys.get(0);
            byte[] end = skeys.get(keyCount - 1);
            Result<List<Result<byte[]>>> r = tair.getRangeValue(ns, pkey, start, end, 0, keyCount, false, opt);
            assertEquals(ResultCode.OK, r.getCode());
            assertEquals(keyCount - 1, r.getResult().size());
            
            Result<List<Result<byte[]>>> r1 = tair.getRangeValue(ns, pkey, null, null, 0, keyCount, false, opt);
            assertEquals(ResultCode.OK, r1.getCode());
            assertEquals(keyCount, r1.getResult().size());
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
        } 
    }
}
