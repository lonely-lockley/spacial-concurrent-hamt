package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestMultiThreadInserts extends TestBase<String> {

    private static final int RETRIES = 10;
    private static final int N_THREADS = 7;
    private static final int COUNT = 50000;

    @Test
    public void testMultiThreadInserts () {
        final var sctm = new SpacialConcurrentTrieMap<String, Integer>();
        for (int j = 0; j < RETRIES; j++) {
            final var testData = new ArrayList<H3CellId<String>>(COUNT);
            for (int i = 0; i < COUNT; i++) {
                var cellId = generateRandomCell(String.valueOf(i));
                while (sctm.containsKey(cellId)) {
                    // had problems with randomness when added resolution boundaries
                    cellId = generateRandomCell(String.valueOf(i));
                }
                testData.add(cellId);
            }
            multiThreadInserts(sctm, testData);
            Assert.assertEquals(((j + 1) * COUNT), sctm.size());
        }
        sctm.clear();
        Assert.assertEquals(0, sctm.size());
        Assert.assertTrue(sctm.isEmpty());
    }

    public void multiThreadInserts(SpacialConcurrentTrieMap<String, Integer> sctm, ArrayList<H3CellId<String>> testData) {
        final ExecutorService es = Executors.newFixedThreadPool(N_THREADS);
        for (int i = 0; i < N_THREADS; i++) {
            final int threadNo = i;
            es.execute(() -> {
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
        catch (final InterruptedException e) {
            e.printStackTrace();
        }

        for (int j = 0; j < COUNT; j++) {
            final Integer lookup = sctm.lookup(testData.get(j));
            Assert.assertEquals(Integer.valueOf(testData.get(j).getBusinessEntityId()), lookup);
        }
    }

}
