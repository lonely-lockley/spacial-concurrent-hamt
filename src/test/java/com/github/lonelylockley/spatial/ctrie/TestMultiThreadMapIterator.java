package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestMultiThreadMapIterator extends TestBase {

    private static final int NTHREADS = 7;
    private static final int COUNT = 500000;

    private final static Map<H3CellId<String>, String> sctm = new SpatialConcurrentTrieMap<>();

    private boolean accepts (final int threadNo, final int nThreads, final H3CellId<String> key) {
        int val = Integer.parseInt(key.getBusinessEntityId());
        return val % nThreads == threadNo;
    }

    @BeforeClass
    public static void setUp() {
        var generator = new TestMultiThreadMapIterator();
        for (int j = 0; j < COUNT; j++) {
            sctm.put(generator.generateRandomCell(String.valueOf(j)), String.valueOf(j));
        }
    }

    @Test(priority = 1)
    public void testMultiThreadMapIteratorSetValue() throws InterruptedException {
        final ExecutorService es = Executors.newFixedThreadPool(NTHREADS);
        for (int i = 0; i < NTHREADS; i++) {
            final int threadNo = i;
            es.execute (() -> {
                for (final var it = sctm.entrySet().iterator (); it.hasNext();) {
                    final Entry<H3CellId<String>, String> e = it.next();
                    if (accepts(threadNo, NTHREADS, e.getKey())) {
                        e.setValue("TEST:" + threadNo);
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(3600L, TimeUnit.SECONDS);
    }

    @Test(priority = 2)
    public void testMultiThreadMapIteratorGetValue() {
        var cnt = 0;
        for (final Entry<H3CellId<String>, String> kv : sctm.entrySet()) {
            var value = kv.getValue();
            Assert.assertTrue(value.startsWith("TEST:"));
            cnt++;
        }
        Assert.assertEquals(COUNT, cnt);
    }

    @Test(priority = 3)
    public void testMultiThreadMapIteratorRemove() throws InterruptedException {
        final ExecutorService es = Executors.newFixedThreadPool(NTHREADS);
        for (int i = 0; i < NTHREADS; i++) {
            final int threadNo = i;
            es.execute (() -> {
                for (final var it = sctm.entrySet().iterator(); it.hasNext();) {
                    final var e = it.next();
                    var key = e.getKey();
                    if (accepts(threadNo, NTHREADS, key)) {
                        Assert.assertNotNull(sctm.get(key));
                        it.remove();
                        Assert.assertNull(sctm.get(key));
                    }
                }
            });
        }

        es.shutdown();
        es.awaitTermination(3600L, TimeUnit.SECONDS);

        Assert.assertEquals(0, sctm.size());
        Assert.assertTrue(sctm.isEmpty());
    }
}
