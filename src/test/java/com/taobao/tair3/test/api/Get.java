package com.taobao.tair3.test.api;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;
import com.taobao.tair3.client.util.TairConstant.MetaFlag;
public class Get extends TestBase {
    @Test
    public void simplePrefixPut() {
        TairOption tairOption = new TairOption(500, (short) 0, 500);
        String _pkey = UUID.randomUUID().toString();
        String _skey = UUID.randomUUID().toString();
        String _val = UUID.randomUUID().toString();
        byte[] pkey = _pkey.getBytes();
        byte[] skey = _skey.getBytes();
        byte[] val = _val.getBytes();
        
        Result<Void> r;
        // init put
        tairOption.setVersion((short) 1);
        try {
            r = tair.prefixPut(ns, pkey, skey, val, tairOption);
            assertEquals(ResultCode.OK, r.getCode());
        } catch (Exception e) {
        }
        short version = 1;
        
        for (int i = 0; i < 5; i++) {
            try {
                tairOption.setVersion((short) 66);
                r = tair.prefixPut(ns, pkey, skey, val, tairOption);
                assertEquals(ResultCode.VERSION_ERROR, r.getCode());
                
                tairOption.setVersion(version);
                r = tair.prefixPut(ns, pkey, skey, val, tairOption);
                assertEquals(ResultCode.OK, r.getCode());
                version ++;

                Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
                assertEquals(ResultCode.OK, g.getCode());
                assertEquals(version, g.getVersion());
                long delta = g.getExpire() - System.currentTimeMillis() / 1000;
                assertTrue(delta < 501 && delta > 490);
                assertArrayEquals(g.getResult(), val);
                assertEquals(pkey.length + skey.length, g.getKey().length);
                assertArrayEquals(pkey, Arrays.copyOfRange(g.getKey(), 0, pkey.length));
                assertArrayEquals(skey, Arrays.copyOfRange(g.getKey(), pkey.length, g.getKey().length));
                
                System.out.print(".");
            } catch (TairRpcError e) {
                e.printStackTrace();
                assertEquals(false, true);
            } catch (TairFlowLimit e) {
                e.printStackTrace();
                assertEquals(false, true);
            } catch (TairTimeout e) {
                e.printStackTrace();
                assertEquals(false, true);
            } catch (InterruptedException e) {
                e.printStackTrace();
                assertEquals(false, true);
            }
        }
    }

    @Test
    public void simpleGet() {
        for (int i = 0; i < 10; ++i) {
            byte[] key = UUID.randomUUID().toString().getBytes();
            byte[] val = UUID.randomUUID().toString().getBytes();
            try {
                Result<Void> r = tair.put(ns, key, val, opt);
                assertEquals(ResultCode.OK, r.getCode());

                Result<byte[]> g = tair.get(ns, key, opt);
                assertEquals(ResultCode.OK, g.getCode());

                assertArrayEquals(key, g.getKey());
                assertArrayEquals(val, g.getResult());
                assertEquals(1, g.getVersion());
                assertTrue(MetaFlag.NEW_META.test(g.getFlag()));

            } catch (TairRpcError e) {
                e.printStackTrace();
                assertEquals(false, true);
            } catch (TairFlowLimit e) {
                e.printStackTrace();
                assertEquals(false, true);
            } catch (TairTimeout e) {
                e.printStackTrace();
                assertEquals(false, true);
            } catch (InterruptedException e) {
                e.printStackTrace();
                assertEquals(false, true);
            }
        }
    }
    
    @Test
    public void simpleGetWithIllegalParameter() {
        
        try {
            byte[] key = UUID.randomUUID().toString().getBytes();
            byte[] val = UUID.randomUUID().toString().getBytes();
            Result<Void> r = tair.put(ns, key, val, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            tair.get(ns, null, opt);
            
        } catch (TairRpcError e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairTimeout e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
        }
        
        try {
            byte[] key = UUID.randomUUID().toString().getBytes();
            byte[] val = UUID.randomUUID().toString().getBytes();
            Result<Void> r = tair.put(ns, key, val, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            tair.get((short)-1, key, null);
            
        } catch (TairRpcError e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairTimeout e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
        }
        
        try {
            byte[] key = UUID.randomUUID().toString().getBytes();
            byte[] val = UUID.randomUUID().toString().getBytes();
            Result<Void> r = tair.put(ns, key, val, null);
            assertEquals(ResultCode.OK, r.getCode());
            
            tair.get((short)TairConstant.NAMESPACE_MAX, key, null);
            
        } catch (TairRpcError e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairTimeout e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
        }
    }
    
    //@Test
    public void simpleGetWithDataNotExist() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> r = tair.invalidByProxy(ns, key, null);
            assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.get(ns, key, null);
            assertEquals(ResultCode.NOTEXISTS, g.getCode());
            
        } catch (TairRpcError e) {
            //assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
        
    @Test
    public void simpleGetWithDataExpired() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        byte[] val = UUID.randomUUID().toString().getBytes();
        try {
            TairOption opt = new TairOption(500, (short) 0, 1);
            Result<Void> r = tair.put(ns, key, val, opt);
            assertEquals(ResultCode.OK, r.getCode());
            Result<byte[]> g = tair.get(ns, key, null);
            assertEquals(ResultCode.OK, g.getCode());
            assertArrayEquals(val, g.getResult());

            Thread.sleep(2000);

            g = tair.get(ns, key, null);
            assertEquals(ResultCode.NOTEXISTS, g.getCode());

        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
    
    @Test
    public void simpleLockUnlock() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        byte[] val = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> r = tair.put(ns, key, val, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.get(ns, key, opt);
            assertEquals(ResultCode.OK, g.getCode());
            
            assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
            assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
            assertEquals(1, g.getVersion());
            assertEquals(MetaFlag.NEW_META.getVal(), g.getFlag());
            
            Result<Void> l = tair.lock(ns, key, opt);
            assertEquals(ResultCode.OK, l.getCode());
            
            // relock should fail
            l = tair.lock(ns, key, opt);
            assertEquals(ResultCode.LOCK_ALREADY_EXIST, l.getCode());
            
            Result<Void> u = tair.unlock(ns, key, opt);
            assertEquals(ResultCode.OK, u.getCode());
            
            // reunlock should fail
            u = tair.unlock(ns, key, opt);
            assertEquals(ResultCode.LOCK_NOT_EXIST, u.getCode());
            
        } catch (TairRpcError e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (TairTimeout e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        }
    }
    
    @Test
    public void simpleGetWithCounter() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        int value = 11;
        int defaultValue = 0;
        try {
            Result<Long> r = tair.incr(ns, key, value, defaultValue, null);
            assertEquals(ResultCode.OK, r.getCode());

            TairOption opt = new TairOption(50000, (short)0, 0);
            Result<byte[]> g = tair.get(ns, key, opt);
            assertEquals(ResultCode.OK, g.getCode());
            assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
            assertEquals(true, g.isCounter());

        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
    
    @Test
    public void simpleGet1() {
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        byte[] skey = UUID.randomUUID().toString().getBytes();
        byte[] value = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
            assertEquals(ResultCode.OK, g.getCode());
            
            assertArrayEquals(value, g.getResult());
            assertEquals(1, g.getVersion());
            assertEquals(MetaFlag.NEW_META.getVal(), g.getFlag());
            
        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
    
    @Test
    public void simpleGetWithIllegalParameter1() {
        
        try {
            byte[] pkey = UUID.randomUUID().toString().getBytes();
            byte[] skey = UUID.randomUUID().toString().getBytes();
            byte[] value = UUID.randomUUID().toString().getBytes();
            Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.prefixGet(ns, null, skey, opt);
            assertEquals(ResultCode.OK, g.getCode());
        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
        }
        
        try {
            byte[] pkey = UUID.randomUUID().toString().getBytes();
            byte[] skey = UUID.randomUUID().toString().getBytes();
            byte[] value = UUID.randomUUID().toString().getBytes();
            Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.prefixGet(ns, pkey, null, opt);
            assertEquals(ResultCode.OK, g.getCode());
        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
        }
        
        try {
            byte[] pkey = UUID.randomUUID().toString().getBytes();
            byte[] skey = UUID.randomUUID().toString().getBytes();
            byte[] value = UUID.randomUUID().toString().getBytes();
            Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.prefixGet((short)-1, pkey, skey, opt);
            assertEquals(ResultCode.OK, g.getCode());
        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
        }
    }
    
    //@Test
    public void simpleGetWithDataNotExist1() {
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        byte[] skey = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> r = tair.prefixInvalidByProxy(ns, pkey, skey, null);
            assertEquals(true, r.getCode().equals(ResultCode.NOTEXISTS) || r.getCode().equals(ResultCode.OK));
            
            Result<byte[]> g = tair.prefixGet(ns, pkey, skey, null);
            assertEquals(ResultCode.NOTEXISTS, g.getCode());
            
        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
        
    @Test
    public void simpleGetWithDataExpired1() {
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        byte[] skey = UUID.randomUUID().toString().getBytes();
        byte[] value = UUID.randomUUID().toString().getBytes();
        try {
            TairOption opt = new TairOption(500, (short) 0, 1);
            Result<Void> r = tair.prefixPut(ns, pkey, skey, value, opt);
            assertEquals(ResultCode.OK, r.getCode());

            Thread.sleep(2000);

            Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
            assertEquals(ResultCode.NOTEXISTS, g.getCode());

        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
    
    @Test
    public void simpleGetWithCounter1() {
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        byte[] skey = UUID.randomUUID().toString().getBytes();
        int value = 11;
        int defaultValue = 0;
        try {
            Result<Long> r = tair.prefixIncr(ns, pkey, skey, value, defaultValue, opt);
            assertEquals(ResultCode.OK, r.getCode());

            TairOption opt = new TairOption(50000, (short)0, 0);
            Result<byte[]> g = tair.prefixGet(ns, pkey, skey, opt);
            assertEquals(ResultCode.OK, g.getCode());

        } catch (TairRpcError e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (TairTimeout e) {
            assertEquals(false, true);
            e.printStackTrace();
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
    
}
