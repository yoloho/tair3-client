package com.taobao.tair3.test.api;
import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.ByteArray;
import com.taobao.tair3.client.util.TairConstant;
public class Delete extends TestBase {
    @Test
    public void simpleDelete() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        byte[] val = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> r = tair.put(ns, key, val, opt);
            assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.get(ns, key, null);
            assertEquals(ResultCode.OK, g.getCode());
            
            assertEquals(new ByteArray(key), new ByteArray(g.getKey()));
            assertEquals(new ByteArray(val), new ByteArray(g.getResult()));
            assertEquals(1, g.getVersion());
            //assertEquals(0, g.getFlag());
            
            Result<Void> d = tair.invalidByProxy(ns, key, null);
            assertEquals(ResultCode.OK, d.getCode());
            
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
    public void simpleDeleteWithNotExist() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        try {
            //Result<Void> r = tair.put(ns, key, val, null);
            //assertEquals(ResultCode.OK, r.getCode());
            
            Result<byte[]> g = tair.get(ns, key, null);
            assertEquals(ResultCode.NOTEXISTS, g.getCode());
            
            /// XXX invalid proxy
            //Result<Void> d = tair.invalidByProxy(ns, key, null);
            //assertEquals(ResultCode.NOTEXISTS, d.getCode());
            
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
            e.printStackTrace();
        }
    }
    
    @Test
    public void simpleDeleteWithExpired() {
        byte[] key = UUID.randomUUID().toString().getBytes();
        byte[] val = UUID.randomUUID().toString().getBytes();
        try {
            Result<Void> r = tair.put(ns, key, val, new TairOption(500, (short)0, 2));
            assertEquals(ResultCode.OK, r.getCode());
            
            Thread.sleep(3000);
            
            Result<byte[]> g = tair.get(ns, key, null);
            assertEquals(ResultCode.NOTEXISTS, g.getCode());
            
            // XXX invalid
            //Result<Void> d = tair.invalidByProxy(ns, key, null);
            //assertEquals(ResultCode.NOTEXISTS, d.getCode());
            
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        }
    }
    
    //@Test
    public void simpleDeleteWithIllegalParameter() {
        try {
            byte[] key = UUID.randomUUID().toString().getBytes();
            byte[] val = UUID.randomUUID().toString().getBytes();
            Result<Void> r = tair.put(ns, key, val, null);
            assertEquals(ResultCode.OK, r.getCode());
            
            tair.invalidByProxy(ns, null, opt);
            
        } catch (TairException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
        }
    }
}
