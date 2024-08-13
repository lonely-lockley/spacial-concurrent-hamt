package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TestMultiThreadSubtree extends TestBase<String> {

    private static final int RETRIES = 10;
    private static final int COUNT = 50000;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock writeLock = readWriteLock.writeLock();
    private final Lock readLock = readWriteLock.readLock();

    private int N_THREADS = 0;

    @Test(priority = 1)
    public void testMoveNodesAndSubTree2Threads() {
        N_THREADS = 2;
        System.out.println("Starting testMoveNodesAndSubTree for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 2)
    public void testMoveNodesAndSubTree4Threads() {
        N_THREADS = 4;
        System.out.println("Starting testMoveNodesAndSubTree for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 3)
    public void testMoveNodesAndSubTree6Threads() {
        N_THREADS = 6;
        System.out.println("Starting testMoveNodesAndSubTree for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 4)
    public void testMoveNodesAndSubTree8Threads() {
        N_THREADS = 8;
        System.out.println("Starting testMoveNodesAndSubTree for " + N_THREADS + " threads");
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println(COUNT + " iterations: " + (System.currentTimeMillis() - start) + "ms");
        }
        System.out.println();
    }

    @Test(priority = 5)
    public void testMoveNodesAndSubTree10Threads() {
        N_THREADS = 10;
        System.out.println("Starting testMoveNodesAndSubTree for " + N_THREADS + " threads");
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
        final SpatialConcurrentTrieMap<String, Integer> sctm = new SpatialConcurrentTrieMap<>();
        final ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        final String[] businessEntities = new String[N_THREADS / 2];
        final AtomicInteger errorCount = new AtomicInteger(0);
        final ConcurrentHashMap<String, H3CellId<String>> businessEntityIndex = new ConcurrentHashMap<>();

        for (int i = 0; i < N_THREADS / 2; i++) {
            var cellId = createCell(String.valueOf(i));
            sctm.put(cellId, i);
            businessEntities[i] = cellId.getBusinessEntityId();
            businessEntityIndex.put(cellId.getBusinessEntityId(), cellId);
        }

        for (int i = 0; i < N_THREADS; i++) {
            final int threadNo = i;
            es.execute (() -> {
                for (int j = 0; j < COUNT; j++) {
                    if (threadNo < N_THREADS / 2) {
                        final var businessEntityId = businessEntities[threadNo];
                        final var cellId = createCell(businessEntityId);
                        var res = false;
                        writeLock.lock();
                        {
                            sctm.put(cellId, sctm.remove(businessEntityIndex.get(businessEntityId)));
                            res = (businessEntityIndex.put(businessEntityId, cellId) != null);
                        }
                        writeLock.unlock();
                        if (!res) {
                            errorCount.incrementAndGet();
                            Assert.fail("businessEntityIndex update was not successful after location update");
                        }
                    }
                    else {
                        final var businessEntityId = businessEntities[threadNo - (N_THREADS / 2)];
                        final var cellId = createCell(businessEntityId);
                        SpatialConcurrentTrieMap<String, Integer> snapshot;
                        readLock.lock();
                        {
                            snapshot = sctm.readOnlySnapshot();
                        }
                        readLock.unlock();
                        SpatialConcurrentTrieMap<String, Integer> res = null;
                        try {
                            res = snapshot.subTree(new H3CellId<>(H3CellId.trimToResolution(cellId.getCellId(), 7), null));
                        }
                        catch (Exception ex) {
                            errorCount.incrementAndGet();
                            Assert.fail("subTree call terminated with an exception", ex);
                        }
                        if (res != null && res.size() != N_THREADS / 2) {
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
        Assert.assertEquals(errorCount.get(), 0, "Errors were found in this iteration. Test run was terminated. Errors");

        for (int i = 0; i < N_THREADS / 2; i++) {
            var cellId = businessEntityIndex.get(businessEntities[i]);
            var res = sctm.remove(cellId);
            Assert.assertEquals(res, i);
        }
    }

}
