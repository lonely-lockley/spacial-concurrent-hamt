package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

public class TestSerialization extends TestBase<String> {

    private static final int SIZE = 10000;

    private final SpacialConcurrentTrieMap<String, Integer> base = new SpacialConcurrentTrieMap<>();
    private SpacialConcurrentTrieMap<String, Integer> deserialized = null;
    private H3CellId<String> cellId = null;
    private Integer value = null;

    @BeforeTest
    public void setUp() throws IOException, ClassNotFoundException {
        base.clear();
        deserialized = null;
        cellId = null;
        value = null;
        var rnd = new Random().nextInt(SIZE);
        for (int i = 0; i < SIZE; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            Assert.assertNull(base.put(cellId, i));
            if (i % rnd == 0) {
                this.cellId = cellId;
                value = i;
            }
        }

        final var expected = base.readOnlySnapshot();
        final var bos = new ByteArrayOutputStream();
        final var oos = new ObjectOutputStream(bos);
        oos.writeObject(expected);
        oos.close();

        final var bytes = bos.toByteArray();
        final var bis = new ByteArrayInputStream(bytes);
        final var ois = new ObjectInputStream(bis);

        @SuppressWarnings("unchecked")
        final var actual = (SpacialConcurrentTrieMap<String, Integer>) ois.readObject();
        this.deserialized = actual;
        ois.close();
    }

    @Test
    public void testSerialization()  {
        Assert.assertFalse(deserialized.isEmpty());
        Assert.assertEquals(base, deserialized);
        Assert.assertEquals(base.size(), deserialized.size());
        for (H3CellId<String> cellId : base.keySet()) {
            Assert.assertTrue(deserialized.containsKey(cellId));
            Assert.assertEquals(deserialized.get(cellId), Integer.valueOf(cellId.getBusinessEntityId()));
        }
    }

    @Test
    public void testDeserializedIsReadOnly()  {
        Assert.assertTrue(deserialized.isReadOnly());
    }

    @Test
    public void testBasicReadMapOperations() {
        Assert.assertFalse(deserialized.isEmpty());
        Assert.assertEquals(value, deserialized.get(cellId));
        Assert.assertTrue(deserialized.containsKey(cellId));
        Assert.assertTrue(deserialized.containsValue(value));
        Assert.assertEquals(SIZE, deserialized.size());
        var entries = deserialized.entrySet();
        Assert.assertEquals(SIZE, entries.size());
        var found = false;
        for (Map.Entry<H3CellId<String>, Integer> entry : entries) {
            if (Objects.equals(cellId, entry.getKey())) {
                Assert.assertEquals(value, entry.getValue());
                found = true;
            }
        }
        Assert.assertTrue(found);
        found = false;
        var keys = deserialized.keySet();
        Assert.assertEquals(SIZE, keys.size());
        for (H3CellId<String> key : keys) {
            if (Objects.equals(cellId, key)) {
                Assert.assertEquals(value, deserialized.get(cellId));
                found = true;
            }
        }
        Assert.assertTrue(found);
        var values = deserialized.values();
        found = false;
        for (Integer val : values) {
            if (Objects.equals(value, val)) {
                found = true;
            }
        }
        Assert.assertTrue(found);
        Assert.assertEquals(value, deserialized.getOrDefault(cellId, 42));
        Assert.assertEquals(Integer.valueOf(42), deserialized.getOrDefault(generateRandomCell("test"), 42));
    }

    @Test
    public void testBasicConcurrentMapOperations() {
        deserialized.forEach((key, value) -> Assert.assertEquals(base.get(key), value));
    }

}
