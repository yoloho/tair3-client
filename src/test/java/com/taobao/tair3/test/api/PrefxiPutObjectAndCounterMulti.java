package com.taobao.tair3.test.api;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.ResultMap;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.TairClient.Counter;
import com.taobao.tair3.client.TairClient.Pair;
import com.taobao.tair3.client.TairClient.RequestOption;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;

public class PrefxiPutObjectAndCounterMulti extends TestBase {
    @Test
    public void simplePrefixPutObjectAndCounterMulti() {
        int keyCount = 20;
        byte[] pkey = UUID.randomUUID().toString().getBytes();
        List<byte[]> skeys = this.generateKeys(keyCount);
        List<byte[]> cskeys = new ArrayList<byte[]>();
        Map<byte[], Pair<byte[], RequestOption>> kvs = new HashMap<byte[], Pair<byte[], RequestOption>>();
        Map<byte[], Pair<Integer, RequestOption>> cvs = new HashMap<byte[], Pair<Integer, RequestOption>> ();
        for (int i = 0; i < keyCount; i++) {
            byte[] skey = skeys.get(i);
            if (i % 2 == 0) {
                kvs.put(skey, new Pair<byte[], RequestOption>(UUID.randomUUID().toString().getBytes(), opt.getRequestOption()));
            } else {
                cvs.put(skey, new Pair<Integer, RequestOption>(0, opt.getRequestOption()));
                cskeys.add(skey);
            }
        }
        int value = 10;
        Map<byte[], Counter> skv = new HashMap<byte[], Counter>();
        for (byte[] skey : cskeys) {
            skv.put(skey, new Counter(value, 0, 500));
        }
        try {
            ResultMap<byte[], Result<Void>> pm = tair.prefixPutMulti(ns, pkey, kvs, cvs, opt);
            assertEquals(ResultCode.OK, pm.getCode());
            for (Map.Entry<byte[], Result<Void>> e : pm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, e.getValue().getCode());
            }
            
            ResultMap<byte[], Result<byte[]>> gm = tair.prefixGetMulti(ns, pkey, skeys, null);
            assertEquals(ResultCode.OK, gm.getCode());
            for (Map.Entry<byte[], Result<byte[]>> e : gm.getResult().entrySet()) {
                assertEquals(ResultCode.OK, e.getValue().getCode());
            }
            
            ResultMap<byte[], Result<Long>> im = tair.prefixIncrMulti(ns, pkey, skv, opt);
            assertEquals(ResultCode.OK, im.getCode());
            for (Map.Entry<byte[], Result<Long>> e : im.getResult().entrySet()) {
                assertEquals(ResultCode.OK, e.getValue().getCode());
                assertEquals(value, e.getValue().getResult().intValue());
            }
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
