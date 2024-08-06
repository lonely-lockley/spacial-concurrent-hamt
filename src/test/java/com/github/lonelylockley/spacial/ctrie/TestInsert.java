package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestInsert extends TestBase<String> {
    @Test
    public void testInsert () {
        final var sctm = new SpacialConcurrentTrieMap<String, Integer>();
        for (int i = 0; i < 10000; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            Assert.assertNull(sctm.put(cellId, i));
            final var lookup = sctm.lookup(cellId);
            Assert.assertEquals(i, lookup);
        }

        Assert.assertEquals(10000, sctm.size());
        Assert.assertFalse(sctm.isEmpty());
    }
}
