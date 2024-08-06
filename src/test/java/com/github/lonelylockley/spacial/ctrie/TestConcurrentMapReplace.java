package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentMap;

public class TestConcurrentMapReplace extends TestBase<String> {
    private static final int COUNT = 50000;

    @Test
    public void testConcurrentMapReplace () {
        final ConcurrentMap<H3CellId<String>, Integer> map = new SpacialConcurrentTrieMap<>();
        for (int i = 0; i < COUNT; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            while (map.containsKey(cellId)) {
                // had problems with randomness when added resolution boundaries
                cellId = generateRandomCell(String.valueOf(i));
            }
            var snapshot = ((SpacialConcurrentTrieMap)map).snapshot();
            Assert.assertNull(map.replace(cellId, -1));
            Assert.assertFalse(map.replace(cellId, i, -2));
            Assert.assertNull(map.put(cellId, i));
            Assert.assertEquals(i, map.replace(cellId, -1));
            Assert.assertFalse(map.replace(cellId, i, -2));
            Assert.assertTrue(map.replace(cellId, -1, i));
        }
    }
}
