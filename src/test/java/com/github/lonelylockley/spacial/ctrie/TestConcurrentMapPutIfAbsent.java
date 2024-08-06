package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentMap;

public class TestConcurrentMapPutIfAbsent extends TestBase<String> {
    private static final int COUNT = 50000;

    @Test
    public void testConcurrentMapPutIfAbsent () {
        final ConcurrentMap<H3CellId<String>, Integer> map = new SpacialConcurrentTrieMap<>();
        for (int i = 0; i < COUNT; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            Assert.assertNull(map.putIfAbsent(cellId, i));
            Assert.assertEquals(i, map.putIfAbsent(cellId, i));
        }
    }
}
