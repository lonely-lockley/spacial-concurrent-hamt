package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestMultiThreadAddDelete extends TestBase<String> {
    private static final int RETRIES = 10;
    private static final int N_THREADS = 7;
    private static final int COUNT = 50000;

    private List<H3CellId<String>> generateTestData() {
        var res = new ArrayList<H3CellId<String>>(COUNT);
        for (int i = 0; i < COUNT; i++) {
            res.add(generateRandomCell(String.valueOf(i)));
        }
        return res;
    }

    @Test
    public void testMultiThreadAdd() {
        var testData = generateTestData();
        for (int i = 0; i < RETRIES; i++) {
            multiThreadAdd(testData);
        }
    }

    public void multiThreadAdd(List<H3CellId<String>> testData) {
        final Map<H3CellId<String>, Integer> sctm = new SpatialConcurrentTrieMap<>();
        final ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        for (int i = 0; i < N_THREADS; i++) {
            final int threadNo = i;
            es.execute (() -> {
                for (int j = 0; j < COUNT; j++) {
                    if (j % N_THREADS == threadNo) {
                        sctm.put(testData.get(j), j);
                    }
                }
            });
        }
        es.shutdown ();
        try {
            es.awaitTermination(3600L, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(COUNT, sctm.size());
        Assert.assertFalse(sctm.isEmpty());
        for (H3CellId<String> cellId : testData) {
            Assert.assertTrue(sctm.containsKey(cellId));
            Assert.assertEquals(cellId.getBusinessEntityId(), String.valueOf(sctm.get(cellId)));
        }
    }

    @Test
    public void testMultiThreadDelete() {
        var testData = generateTestData();
        for (int i = 0; i < RETRIES; i++) {
            multiThreadDelete(testData);
        }
    }

    public void multiThreadDelete(List<H3CellId<String>> testData) {
        final Map<H3CellId<String>, Integer> sctm = new SpatialConcurrentTrieMap<>();
        for (H3CellId<String> cellId : testData) {
            sctm.put(cellId, cellId.getBaseCell());
        }

        final ExecutorService es = Executors.newFixedThreadPool (N_THREADS);
        for (int i = 0; i < N_THREADS; i++) {
            final int threadNo = i;
            es.execute(() -> {
                for (int j = 0; j < COUNT; j++) {
                    if (j % N_THREADS == threadNo) {
                        sctm.remove(testData.get(j));
                    }
                }
            });
        }
        es.shutdown();
        try {
            es.awaitTermination(3600L, TimeUnit.SECONDS);
        }
        catch (final InterruptedException e) {
            e.printStackTrace ();
        }

        Assert.assertEquals(0, sctm.size ());
        Assert.assertTrue(sctm.isEmpty());
    }

    @Test
    public void testMultiThreadAddDelete() {
        var testData = generateTestData();
        for (int i = 0; i < RETRIES; i++) {
            multiThreadAddDelete(testData);
        }
    }

    public void multiThreadAddDelete(List<H3CellId<String>> testData) {
        final Map<H3CellId<String>, Integer> sctm = new SpatialConcurrentTrieMap<>();
        for (H3CellId<String> cellId : testData) {
            sctm.put(cellId, cellId.getBaseCell());
        }

        final ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        for (int i = 0; i < N_THREADS; i++) {
            final int threadNo = i;
            es.execute(() -> {
                for (int j = 0; j < COUNT; j++) {
                    if (j % N_THREADS == threadNo) {
                        try {
                            sctm.put(testData.get(j), j);
                            if (!sctm.containsKey(testData.get(j))) {
                                System.out.println(j);
                            }
                            sctm.remove(testData.get(j));
                            if (sctm.containsKey(testData.get(j))) {
                                System.out.println(-j);
                            }
                        }
                        catch (Throwable t) {
                            t.printStackTrace();
                        }
                    }
                }
            });
        }
        es.shutdown();
        try {
            es.awaitTermination(3600L, TimeUnit.SECONDS);
        }
        catch (final InterruptedException e) {
            e.printStackTrace();
        }

        Assert.assertEquals(0, sctm.size());
        Assert.assertTrue(sctm.isEmpty());
    }
}
