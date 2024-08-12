package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class TestHashCollisionsRemoveIterator extends TestBase<String> {
    @Test
    public void testHashCollisionsRemoveIterator () {
        final Map<H3CellId<String>, Integer> sctm = new SpatialConcurrentTrieMap<String, Integer>();
        int count = 50000;
        for (int j = 0; j < count; j++) {
            sctm.put(generateRandomCell(String.valueOf(j)), j);
        }
        
        final Set<H3CellId> keys = new HashSet<>();
        for (final var i = sctm.entrySet().iterator(); i.hasNext();) {
            final Entry<H3CellId<String>, Integer> e = i.next();
            keys.add(e.getKey());
            i.remove();
        }

        Assert.assertEquals (0, sctm.size());
        Assert.assertTrue (sctm.isEmpty());
        Assert.assertEquals (50000, keys.size());
    }
}
