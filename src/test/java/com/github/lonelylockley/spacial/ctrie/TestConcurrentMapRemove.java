package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentMap;

public class TestConcurrentMapRemove extends TestBase<String> {
    private static final int COUNT = 50000;

    @Test
    public void testConcurrentMapRemoveRandom() {
        final ConcurrentMap<H3CellId<String>, Integer> map = new SpacialConcurrentTrieMap<>();
        for (int i = 128; i < COUNT; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            Assert.assertFalse(map.remove(cellId, i));
            Assert.assertNull(map.put(cellId, i));
            Assert.assertFalse(map.remove(cellId, -1));
            Assert.assertTrue(map.containsKey(cellId));
            Assert.assertTrue(map.remove(cellId, i));
            Assert.assertFalse(map.containsKey(cellId));
            Assert.assertNull(map.put(cellId, i));
        }
    }

}
