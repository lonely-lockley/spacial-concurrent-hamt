package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

public class TestMapIterator extends TestBase<String> {
    @Test
    public void testMapIterator () {
        for (int i = 0; i < 60000; i+= 400 + new Random ().nextInt (400)) {
            final Map<H3CellId<String>, Integer> sctm = new SpatialConcurrentTrieMap<>();
            for (int j = 0; j < i; j++) {
                Assert.assertNull(sctm.put(generateRandomCell(String.valueOf(j)), j));
            }

            int count = 0;
            final var set = new HashSet<H3CellId<String>>();
            for (final Entry<H3CellId<String>, Integer> e : sctm.entrySet()) {
                set.add(e.getKey());
                count++;
            }
            for (final H3CellId<String> cellId : set) {
                Assert.assertTrue(sctm.containsKey(cellId));
            }
            for (final H3CellId<String> j : sctm.keySet()) {
                Assert.assertTrue(set.contains(j));
            }

            Assert.assertEquals(i, count);
            Assert.assertEquals(i, sctm.size());
            
            for (final var iter = sctm.entrySet().iterator(); iter.hasNext();) {
                final Entry<H3CellId<String>, Integer> e = iter.next();
                Assert.assertEquals(e.getValue(), sctm.get(e.getKey()));
                Integer expected = e.getValue () + 1;
                e.setValue(expected);
                Assert.assertEquals(expected, sctm.get(e.getKey()));
                e.setValue(e.getValue() - 1);
            }

            for (final Iterator<H3CellId<String>> iter = sctm.keySet().iterator(); iter.hasNext();) {
                final var k = iter.next();
                Assert.assertTrue(sctm.containsKey(k));
                iter.remove();
                Assert.assertFalse(sctm.containsKey(k));
            }
            
            Assert.assertEquals(0, sctm.size());
            Assert.assertTrue(sctm.isEmpty());
        }
    }
}
