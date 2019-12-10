package com.taobao.tair3.test.api;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;
import com.taobao.tair3.client.util.TairConstant;

public class BatchDelete extends TestBase {
    protected int keyCount = 20;
    @Test
    public void simpleBatchPutAndGet() {
        List<byte[]> keys = generateKeys(10);
        Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
        for (byte[] key : keys) {
            kvs.put(key, UUID.randomUUID().toString().getBytes());
        }
        try {
            ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
            assertEquals(ResultCode.OK, bp.getCode());
            assertEquals(kvs.size(), bp.getResult().size());
            for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, keys, null);
            assertEquals(ResultCode.OK, bg.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<Void>> bd = tair.batchInvalidByProxy(ns, keys, null);
            assertEquals(ResultCode.OK, bd.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : bd.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> bg1 = tair.batchGet(ns, keys, null);
            //server's bug
            assertEquals(ResultCode.PART_OK, bg1.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : bg1.getResult().entrySet()) {
                assertEquals(true, ResultCode.OK.equals(entry.getValue().getCode()) || ResultCode.NOTEXISTS.equals(entry.getValue().getCode()));
            }
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        }
    }
    @Test
    public void simpleBatchPutWithIllegalInput() {
        List<byte[]> keys = generateKeys(10);
        Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
        for (byte[] key : keys) {
            kvs.put(key, UUID.randomUUID().toString().getBytes());
        }
        try {
            ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
            assertEquals(ResultCode.OK, bp.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, keys, null);
            assertEquals(ResultCode.OK, bg.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<Void>> bd = tair.batchInvalidByProxy(ns, null, null);
            assertEquals(ResultCode.OK, bd.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : bd.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
        } catch (TairRpcError e) {
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
        } catch (TairTimeout e) {
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
        }
        
        try {
            ResultMap<byte[], Result<Void>> bp = tair.batchPut((short)-1, kvs, null);
            assertEquals(ResultCode.OK, bp.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
        } catch (TairRpcError e) {
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
        } catch (TairTimeout e) {
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
        }
        
        try {
            ResultMap<byte[], Result<Void>> bp = tair.batchPut(ns, kvs, null);
            assertEquals(ResultCode.OK, bp.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : bp.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, keys, null);
            assertEquals(ResultCode.OK, bg.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
            
            ResultMap<byte[], Result<Void>> bd = tair.batchInvalidByProxy((short)-1, keys, null);
            assertEquals(ResultCode.OK, bd.getCode());
            for (Map.Entry<byte[], Result<Void>> entry : bd.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
        } catch (TairRpcError e) {
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
        } catch (TairTimeout e) {
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
        }
    }
    
    @Test
    public void simpleBatchGetWithIllegalInput() {
        List<byte[]> keys = generateKeys(10);
        Map<byte[], byte[]> kvs = new HashMap<byte[], byte[]>();
        for (byte[] key : keys) {
            kvs.put(key, null);
        }
        try {
            ResultMap<byte[], Result<byte[]>> bg = tair.batchGet(ns, null, null);
            assertEquals(ResultCode.OK, bg.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
        } catch (TairRpcError e) {
            assertEquals(false, true);
        } catch (TairFlowLimit e) {
            assertEquals(false, true);
        } catch (TairTimeout e) {
            assertEquals(false, true);
        } catch (InterruptedException e) {
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.KEY_NOT_AVAILABLE, e.getMessage());
        }
        
        try {
            ResultMap<byte[], Result<byte[]>> bg = tair.batchGet((short)-1, keys, null);
            assertEquals(ResultCode.OK, bg.getCode());
            for (Map.Entry<byte[], Result<byte[]>> entry : bg.getResult().entrySet()) {
                assertEquals(ResultCode.OK, entry.getValue().getCode());
            }
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (IllegalArgumentException e) {
            assertEquals(TairConstant.NS_NOT_AVAILABLE, e.getMessage());
        }
    }
}
