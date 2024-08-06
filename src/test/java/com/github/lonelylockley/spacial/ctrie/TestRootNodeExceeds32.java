package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class TestRootNodeExceeds32 extends TestBase<String> {
    @Test
    public void testRootNodeExceeds32() {
        final ConcurrentMap<H3CellId<String>, Integer> map = new SpacialConcurrentTrieMap<>();
        var size = 122;
        List<H3CellId<String>> values = new ArrayList<>(size * 2);
        for (int i = 0; i < size; i++) {
            var cellId = generateNonRandomCell(i, String.valueOf(i));
            map.put(cellId, i);
            values.add(cellId);
        }
        Assert.assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            Assert.assertEquals(i, map.get(values.get(i)));
        }
    }
}
