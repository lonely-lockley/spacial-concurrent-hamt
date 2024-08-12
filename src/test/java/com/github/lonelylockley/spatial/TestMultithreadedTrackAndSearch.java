package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class TestMultithreadedTrackAndSearch extends TestBase<String> {

    private static final int RETRIES = 10;
    private static final int N_THREADS = 10;
    private static final int COUNT = 50000;

    @Test
    public void testNoDuplicates() {
        for (int i = 0; i < RETRIES; i++) {
            var start = System.currentTimeMillis();
            multiThreadedTrackAndSearch();
            System.out.println("50000 iterations: " + (System.currentTimeMillis() - start));
        }
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
                            Assert.assertTrue(res);
                        }
                    }
                    else {
                        final var businessEntityId = businessEntities[threadNo - (N_THREADS / 2)];
                        final var cellId = createCell(businessEntityId);
                        var res = trk.findAround(cellId.getCellId(), (cid, value) -> true, 7, 0, 100);
                        if (res.size() != N_THREADS / 2) {
                            errorCount.incrementAndGet();
                            Assert.assertEquals(res.size(), N_THREADS / 2);
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
