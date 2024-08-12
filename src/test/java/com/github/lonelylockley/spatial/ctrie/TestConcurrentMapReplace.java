package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentMap;

public class TestConcurrentMapReplace extends TestBase<String> {
    private static final int COUNT = 50000;

    @Test
    public void testConcurrentMapReplace () {
        final ConcurrentMap<H3CellId<String>, Integer> map = new SpatialConcurrentTrieMap<>();
        for (int i = 0; i < COUNT; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            while (map.containsKey(cellId)) {
                // had problems with randomness when added resolution boundaries
                cellId = generateRandomCell(String.valueOf(i));
            }
            var snapshot = ((SpatialConcurrentTrieMap)map).snapshot();
            Assert.assertNull(map.replace(cellId, -1));
            Assert.assertFalse(map.replace(cellId, i, -2));
            Assert.assertNull(map.put(cellId, i));
            Assert.assertEquals(i, map.replace(cellId, -1));
            Assert.assertFalse(map.replace(cellId, i, -2));
            Assert.assertTrue(map.replace(cellId, -1, i));
        }
    }
}
