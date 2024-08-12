package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.StreamSupport;

public class TestBasicMapOps extends TestBase<String> {

    @Test
    public void testMapOps() {
        Map<H3CellId<String>, Integer> sctm = new SpatialConcurrentTrieMap<>();
        var cellId = generateRandomCell("test");
        Assert.assertTrue(sctm.isEmpty());
        Assert.assertNull(sctm.put(cellId, 100500));
        Assert.assertEquals(100500, sctm.put(cellId, 42));
        Assert.assertEquals(42, sctm.get(cellId));
        Assert.assertTrue(sctm.containsKey(cellId));
        Assert.assertTrue(sctm.containsValue(42));
        Assert.assertNull(sctm.replace(generateRandomCell("rrr"), 100500));
        Assert.assertEquals(42, sctm.replace(cellId, 100500));
        Assert.assertFalse(sctm.replace(cellId, 66, 42));
        Assert.assertTrue(sctm.replace(cellId, 100500, 42));
        Assert.assertFalse(sctm.isEmpty());
        Assert.assertEquals(1, sctm.size());
        var entries = sctm.entrySet();
        Assert.assertEquals(1, entries.size());
        for (Map.Entry<H3CellId<String>, Integer> entry : entries) {
            Assert.assertEquals(cellId, entry.getKey());
            Assert.assertEquals(42, entry.getValue());
        }
        var keys = sctm.keySet();
        Assert.assertEquals(1, keys.size());
        for (H3CellId<String> key : keys) {
            Assert.assertEquals(cellId, key);
        }
        var values = sctm.values();
        Assert.assertEquals(1, values.size());
        for (Integer value : values) {
            Assert.assertEquals(42, value);
        }
        sctm.clear();
        Assert.assertTrue(sctm.isEmpty());
        Assert.assertEquals(-1, sctm.compute(cellId, (key, value) -> value == null ? -1 : value));
        sctm.put(cellId, 42);
        Assert.assertEquals(42, sctm.compute(cellId, (key, value) -> value == null ? -1 : value));
        Assert.assertEquals(42, sctm.computeIfAbsent(cellId, key -> 100500));
        sctm.remove(cellId);
        sctm.remove(cellId);
        Assert.assertEquals(100500, sctm.computeIfAbsent(cellId, key -> 100500));
        Assert.assertEquals(42, sctm.computeIfPresent(cellId, (key, value) -> 42));
        sctm.remove(cellId);
        Assert.assertNull(sctm.computeIfPresent(cellId, (key, value) -> 100500));
        Assert.assertEquals(42, sctm.getOrDefault(cellId, 42));
        Assert.assertNull(sctm.putIfAbsent(cellId, 100500));
        Assert.assertEquals(100500, sctm.getOrDefault(cellId, 42));
        Assert.assertEquals(100500, sctm.remove(cellId));
        var collection = new HashMap<H3CellId<String>, Integer>();
        for (int i = 0; i < 10; i++) {
            collection.put(generateRandomCell(String.valueOf(i)), i);
        }
        sctm.putAll(collection);
        Assert.assertEquals(collection.size(), sctm.size());
        for (Map.Entry<H3CellId<String>, Integer> entry : collection.entrySet()) {
            Assert.assertTrue(sctm.containsKey(entry.getKey()));
            Assert.assertEquals(entry.getValue(), sctm.get(entry.getKey()));
        }
        sctm.replaceAll((key, value) -> value + 1);
        for (Map.Entry<H3CellId<String>, Integer> entry : collection.entrySet()) {
            Assert.assertTrue(sctm.containsKey(entry.getKey()));
            Assert.assertEquals(entry.getValue() + 1, sctm.get(entry.getKey()));
        }
        sctm.clear();
        Assert.assertEquals(42, sctm.merge(cellId, 42, (key, value) -> 100500));
        Assert.assertEquals(100500, sctm.merge(cellId, 42, (key, value) -> 100500));
        Assert.assertNull(sctm.merge(cellId, 42, (key, value) -> null));
        Assert.assertEquals(0, sctm.size());
        Assert.assertTrue(sctm.isEmpty());
    }

    @Test
    public void testConcurrentMapOps() {
        final ConcurrentMap<H3CellId<String>, Integer> sctm = new SpatialConcurrentTrieMap<>();
        for (int i = 0; i < 10; i++) {
            sctm.put(generateRandomCell(String.valueOf(i)), i);
        }
        sctm.forEach((key, value) -> Assert.assertEquals(value, sctm.remove(key)));
        Assert.assertTrue(sctm.isEmpty());
    }

    @Test
    public void testBasicAddOpsLongScenario() {
        var tt1 = new H3CellId<>("8f8e62c1d8ddc53", "100501");
        var tt2 = new H3CellId<>("8f8e62c1d8ddc72", "100502");
        var tt3 = new H3CellId<>("8f8e62c1d8ddc0e", "100503");
        var tt4 = new H3CellId<>("8f8e62c1d8ddcc5", "100504");
        var tt5 = new H3CellId<>("8f8e62c1d8ddcc5", "100505");

        var t = new SpatialConcurrentTrieMap<String, String>();

        Assert.assertNull(t.remove(tt1));

        t.add(tt1, "test1");
        t.add(tt2, "test2");
        t.add(tt3, "test3");
        t.add(tt4, "test4");
        t.add(tt5, "test5");

        // =========================================
        Assert.assertEquals(5, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertTrue(t.containsKey(tt2));
        Assert.assertTrue(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        var values = StreamSupport
                        .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                        .map(Map.Entry::getKey)
                        .toArray();
        Assert.assertEquals(new Object[] {tt3, tt1, tt2, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertEquals("test2", t.get(tt2));
        Assert.assertEquals("test3", t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        Assert.assertEquals("test3", t.remove(tt3));
        Assert.assertEquals(4, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertTrue(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt2, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertEquals("test2", t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        t.add(tt2, "test22");
        Assert.assertEquals(4, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertTrue(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt2, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertEquals("test22", t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        Assert.assertEquals("test22", t.remove(tt2));
        Assert.assertEquals(3, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        Assert.assertEquals("test5", t.remove(tt5));
        Assert.assertEquals(2, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt4}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertNull(t.get(tt5));

        // =========================================
        Assert.assertEquals("test1", t.remove(tt1));
        Assert.assertEquals(1, t.size());
        Assert.assertFalse(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt4}, values);
        Assert.assertNull(t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertNull(t.get(tt5));

        // =========================================
        Assert.assertNull(t.remove(tt1));
        Assert.assertEquals(1, t.size());
        Assert.assertFalse(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt4}, values);
        Assert.assertNull(t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertNull(t.get(tt5));

        // =========================================
        Assert.assertEquals("test4", t.remove(tt4));
        Assert.assertEquals(0, t.size());
        Assert.assertFalse(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertFalse(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[0], values);
        Assert.assertNull(t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertNull(t.get(tt4));
        Assert.assertNull(t.get(tt5));
    }

    @Test
    public void testBasicPutOpsLongScenario() {
        var tt1 = new H3CellId<>("8f8e62c1d8ddc53", "100501");
        var tt2 = new H3CellId<>("8f8e62c1d8ddc72", "100502");
        var tt3 = new H3CellId<>("8f8e62c1d8ddc0e", "100503");
        var tt4 = new H3CellId<>("8f8e62c1d8ddcc5", "100504");
        var tt5 = new H3CellId<>("8f8e62c1d8ddcc5", "100505");

        var t = new SpatialConcurrentTrieMap<String, String>();

        Assert.assertNull(t.remove(tt1));

        t.put(tt1, "test1");
        t.put(tt2, "test2");
        t.put(tt3, "test3");
        t.put(tt4, "test4");
        t.put(tt5, "test5");

        // =========================================
        Assert.assertEquals(5, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertTrue(t.containsKey(tt2));
        Assert.assertTrue(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        var values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt3, tt1, tt2, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertEquals("test2", t.get(tt2));
        Assert.assertEquals("test3", t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        Assert.assertEquals("test3", t.remove(tt3));
        Assert.assertEquals(4, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertTrue(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt2, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertEquals("test2", t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        t.put(tt2, "test22");
        Assert.assertEquals(4, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertTrue(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt2, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertEquals("test22", t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        Assert.assertEquals("test22", t.remove(tt2));
        Assert.assertEquals(3, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertTrue(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt4, tt5}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertEquals("test5", t.get(tt5));

        // =========================================
        Assert.assertEquals("test5", t.remove(tt5));
        Assert.assertEquals(2, t.size());
        Assert.assertTrue(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt1, tt4}, values);
        Assert.assertEquals("test1", t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertNull(t.get(tt5));

        // =========================================
        Assert.assertEquals("test1", t.remove(tt1));
        Assert.assertEquals(1, t.size());
        Assert.assertFalse(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt4}, values);
        Assert.assertNull(t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertNull(t.get(tt5));

        // =========================================
        Assert.assertNull(t.remove(tt1));
        Assert.assertEquals(1, t.size());
        Assert.assertFalse(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertTrue(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[] {tt4}, values);
        Assert.assertNull(t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertEquals("test4", t.get(tt4));
        Assert.assertNull(t.get(tt5));

        // =========================================
        Assert.assertEquals("test4", t.remove(tt4));
        Assert.assertEquals(0, t.size());
        Assert.assertFalse(t.containsKey(tt1));
        Assert.assertFalse(t.containsKey(tt2));
        Assert.assertFalse(t.containsKey(tt3));
        Assert.assertFalse(t.containsKey(tt4));
        Assert.assertFalse(t.containsKey(tt5));

        values = StreamSupport
                .stream(Spliterators.spliteratorUnknownSize(t.iterator(), Spliterator.ORDERED), false)
                .map(Map.Entry::getKey)
                .toArray();
        Assert.assertEquals(new Object[0], values);
        Assert.assertNull(t.get(tt1));
        Assert.assertNull(t.get(tt2));
        Assert.assertNull(t.get(tt3));
        Assert.assertNull(t.get(tt4));
        Assert.assertNull(t.get(tt5));
    }

}
