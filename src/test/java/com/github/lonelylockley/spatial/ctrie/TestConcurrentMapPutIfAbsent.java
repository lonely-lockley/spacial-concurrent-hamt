package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentMap;

public class TestConcurrentMapPutIfAbsent extends TestBase<String> {
    private static final int COUNT = 50000;

    @Test
    public void testConcurrentMapPutIfAbsent () {
        final ConcurrentMap<H3CellId<String>, Integer> map = new SpatialConcurrentTrieMap<>();
        for (int i = 0; i < COUNT; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            Assert.assertNull(map.putIfAbsent(cellId, i));
            Assert.assertEquals(i, map.putIfAbsent(cellId, i));
        }
    }
}
