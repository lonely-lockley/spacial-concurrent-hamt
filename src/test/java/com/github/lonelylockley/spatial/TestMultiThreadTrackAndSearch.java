package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMultiThreadTrackAndSearch extends TestBase<String> {

    private static final int RETRIES = 5;
    private static final int COUNT = 50000;

    private int N_THREADS = 0;

    @Test(priority = 1)
    public void testNoDuplicates2Theads() {
        N_THREADS = 2;
        System.out.println("Starting testNoDuplicates for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 2)
    public void testNoDuplicates4Theads() {
        N_THREADS = 4;
        System.out.println("Starting testNoDuplicates for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 3)
    public void testNoDuplicates6Theads() {
        N_THREADS = 6;
        System.out.println("Starting testNoDuplicates for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 4)
    public void testNoDuplicates8Theads() {
        N_THREADS = 8;
        System.out.println("Starting testNoDuplicates for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 5)
    public void testNoDuplicates10Theads() {
        N_THREADS = 10;
        System.out.println("Starting testNoDuplicates for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    private H3CellId<String> createCell(String businessEntityId) {
        return generateNonRandomCell(31, new int[] {1, 4, 2, 4, 3, 4, 2, 7, 7, 7, 7, 7, 7, 7, 7}, 7, 15, businessEntityId);
    }

    private void multiThreadedTrackAndSearch() {
        final Tracker<String, Integer> trk = new LocationTracker<>();
        final ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        final String[] businessEntities = new String[N_THREADS / 2];
        final AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < N_THREADS / 2; i++) {
            var cellId = createCell(String.valueOf(i));
            trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i);
            businessEntities[i] = cellId.getBusinessEntityId();
        }

        for (int i = 0; i < N_THREADS; i++) {
            final int threadNo = i;
            es.execute (() -> {
                for (int j = 0; j < COUNT; j++) {
                    if (threadNo < N_THREADS / 2) {
                        final var businessEntityId = businessEntities[threadNo];
                        final var cellId = createCell(businessEntityId);
                        var res = trk.updateLocation(businessEntityId, cellId);
                        //trk.updateValue(businessEntityId, j);
                        if (!res) {
                            errorCount.incrementAndGet();
                            Assert.fail("businessEntityIndex update was not successful after location update");
                        }
                    }
                    else {
                        final var businessEntityId = businessEntities[threadNo - (N_THREADS / 2)];
                        final var cellId = createCell(businessEntityId);
                        var res = trk.findAround(cellId.getCellId(), (cid, value) -> true, 7, 0, 100);
                        if (res.size() != N_THREADS / 2) {
                            errorCount.incrementAndGet();
                            Assert.fail("subTree result returned unexpected set size. Expected [" + (N_THREADS / 2) + "] but got [" + res.size() + "]");
                        }
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
        Assert.assertEquals(errorCount.get(), 0);

        for (int i = 0; i < N_THREADS / 2; i++) {
            var res = trk.finishTracking(businessEntities[i]);
            Assert.assertEquals(res, i);
        }
    }

}
