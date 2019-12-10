package com.taobao.tair3.test.api;

import static org.junit.Assert.assertEquals;

import java.util.UUID;

import org.junit.Test;

import com.taobao.tair3.client.Result;
import com.taobao.tair3.client.Result.ResultCode;
import com.taobao.tair3.client.error.TairException;
import com.taobao.tair3.client.error.TairFlowLimit;
import com.taobao.tair3.client.error.TairRpcError;
import com.taobao.tair3.client.error.TairTimeout;

public class BoundedIncrDecr extends TestBase {
    protected static long lowBound = -100;
    protected static long upperBound = 100;

    @Test
    public void normalIncr() {
        // 1. create a counter
        String key = UUID.randomUUID().toString();

        try {
            tair.invalidByProxy(ns, key.getBytes(), null);
            Result<Long> i = tair.incr(ns, key.getBytes(), 0, 0, lowBound, upperBound, null);
            assertEquals(ResultCode.OK, i.getCode());
            assertEquals(0, i.getResult().intValue());
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

        for (int k = 0; k < upperBound * 2; ++k) {
            try {
                Result<Long> rr = tair.incr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
                // ok
                if (k < upperBound) {
                    assertEquals(ResultCode.OK, rr.getCode());
                    assertEquals(k + 1, rr.getResult().intValue());
                }
                // out of range
                else {
                    assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
                }
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

    @Test
    public void normalDecr() {
        // 1. create a counter
        byte[] key = UUID.randomUUID().toString().getBytes();
        try {
            tair.invalidByProxy(ns, key, null);
            Result<Long> i = tair.decr(ns, key, 0, 0, lowBound, upperBound, opt);
            assertEquals(ResultCode.OK, i.getCode());
            assertEquals(new Long(0), i.getResult());
        } catch (TairException e) {
            e.printStackTrace();
            assertEquals(false, true);
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertEquals(false, true);
        }

        for (int k = 0; k < upperBound * 2; ++k) {
            try {
                Result<Long> rr = tair.decr(ns, key, 1, 0, lowBound, upperBound, null);
                // ok
                if (k < upperBound) {
                    assertEquals(ResultCode.OK, rr.getCode());
                    assertEquals(-(k + 1), rr.getResult().intValue());
                } else {
                    // out of range
                    assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
                }
            } catch (TairException e) {
                e.printStackTrace();
                assertEquals(false, true);
            } catch (InterruptedException e) {
                e.printStackTrace();
                assertEquals(false, true);
            }
        }
    }

    @Test
    public void boundEqu() {
        String key = UUID.randomUUID().toString();

        int lowBound = 0;
        int upperBound = 0;

        try {
            tair.invalidByProxy(ns, key.getBytes(), null);
            Result<Long> rc = tair.decr(ns, key.getBytes(), 0, 0, lowBound, upperBound, null);
            assertEquals(ResultCode.OK, rc.getCode());
            assertEquals(0, rc.getResult().intValue());

            Result<Long> rc1 = tair.decr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
            assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rc1.getCode());
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
    public void boundNeg() {
        String key = UUID.randomUUID().toString();
        int lowBound = 10;
        int upperBound = -10;
        try {
            tair.invalidByProxy(ns, key.getBytes(), null);
            // Result<Integer> rc =
            tair.decr(ns, key.getBytes(), 0, 0, lowBound, upperBound, null);
            // assertEquals(ResultCode.INVALID_ARGUMENT, rc.getCode());

            // Result<Integer> rc1 = tair.incr(namespace, key, 0, 0, 0,
            // lowBound, upperBound);
            // assertEquals(rc1.getRc(), ResultCode.SERIALIZEERROR);
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
            assertEquals(true, true);
        }
        // assertEquals(ResultCode.);

        // Result<Integer> rc1 = tair.incr(namespace, key, 0, 0, 0, lowBound,
        // upperBound);
        // assertEquals(rc1.getRc(), ResultCode.SERIALIZEERROR);
    }

    @Test
    public void ExistKeyDecr() {
        String key = UUID.randomUUID().toString();
        // removeKey(key);
        Integer x = 0;
        try {
            tair.invalidByProxy(ns, key.getBytes(), null);
            Result<Void> rp = tair.put(ns, key.getBytes(), x.toString().getBytes(), null);
            assertEquals(ResultCode.OK, rp.getCode());

            Result<byte[]> rg = tair.get(ns, key.getBytes(), null);
            assertEquals(ResultCode.OK, rg.getCode());
            // assertEquals(x.toString().getBytes(), rg.getResult());

            assertEquals(ResultCode.OK, tair.setCount(ns, key.getBytes(), 0, null).getCode());
            for (int i = 0; i < upperBound * 2; ++i) {
                Result<Long> rr = tair.decr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
                if (i < upperBound) {
                    assertEquals(ResultCode.OK, rr.getCode());
                    assertEquals(-(i + 1), rr.getResult().intValue());
                } else {
                    // out of range
                    assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
                }
            }
        } catch (TairRpcError e) {
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            e.printStackTrace();
        } catch (TairTimeout e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void ExistKeyIncr() {
        String key = UUID.randomUUID().toString();
        // removeKey(key);
        Integer x = 0;
        try {
            tair.invalidByProxy(ns, key.getBytes(), null);
            Result<Void> rp = tair.put(ns, key.getBytes(), x.toString().getBytes(), null);
            assertEquals(ResultCode.OK, rp.getCode());

            Result<byte[]> rg = tair.get(ns, key.getBytes(), null);
            assertEquals(ResultCode.OK, rg.getCode());
            // assertEquals(x.toString().getBytes(), rg.getResult());

            assertEquals(ResultCode.OK, tair.setCount(ns, key.getBytes(), 0, null).getCode());
            for (int i = 0; i < upperBound * 2; ++i) {
                Result<Long> rr = tair.incr(ns, key.getBytes(), 1, 0, lowBound, upperBound, null);
                if (i < upperBound) {
                    assertEquals(ResultCode.OK, rr.getCode());
                    assertEquals(i + 1, rr.getResult().intValue());
                }
                // out of range
                else {
                    assertEquals(ResultCode.COUNTER_OUT_OF_RANGE, rr.getCode());
                }
            }
        } catch (TairRpcError e) {
            e.printStackTrace();
        } catch (TairFlowLimit e) {
            e.printStackTrace();
        } catch (TairTimeout e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
