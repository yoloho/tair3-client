package com.taobao.tair3.test.api;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;

import com.taobao.tair3.client.TairClient.TairOption;
import com.taobao.tair3.client.impl.DefaultTairClient;
public class TestBase {
    protected String master = "192.168.124.4:5198"; // tair master cs address, for example, 10.232.4.14:5008;
    protected String slave = "192.168.124.4:5199"; // tair slave cs address
    protected String group = "test"; // tair group name
    protected DefaultTairClient tair = null;
    protected short ns = 120; //namespace
    protected TairOption opt = new TairOption(500, (short)0, 500);
    
    @Before
    public void setUp() throws Exception {
        tair = new DefaultTairClient();
        tair.setMaster(master);
        tair.setSlave(slave);
        tair.setGroup(group);
        tair.init();
    }

    @After
    public void tearDown() throws Exception {
         tair.close();
    }
    
    protected List<byte[]> generateKeys(int count) {
        List<byte[]> r = new ArrayList<byte[]> ();
        for (int i = 0; i < count; ++i) {
            r.add(UUID.randomUUID().toString().getBytes());
        }
        return r;
    }
    
    protected List<byte[]> generateOrderedKeys(byte[] key, int count) {
        List<byte[]> result = new ArrayList<byte[]>(count);
        for (int i = 1; i <= count; i ++) {
            String str = String.format("-%05d", i);
            byte[] arr = new byte[key.length + str.length()];
            System.arraycopy(key, 0, arr, 0, key.length);
            System.arraycopy(str.getBytes(), 0, arr, key.length, str.length());
            result.add(arr);
        }
        return result;
    }
}
