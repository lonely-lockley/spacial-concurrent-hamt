package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import com.uber.h3core.H3Core;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

public class TestSubtree extends TestBase<String> {

    private static final int SIZE = 50000;

    @Test
    public void testSubtreeBaseCellCollision() {
        for (int k = 0; k < 100; k++) {
            final var sctm = new SpacialConcurrentTrieMap<String, Integer>();
            for (int i = 0; i < SIZE; i++) {
                var cellId = generateRandomCell(String.valueOf(i));
                sctm.put(cellId, i);
            }
            var originalCell = generateNonRandomCell(31, new int[]{1, 4, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 2, null);
            var res = sctm.subTree(originalCell);
            var matching = sctm
                    .entrySet()
                    .stream()
                    .filter(kv ->
                        kv.getKey().getBaseCell() == originalCell.getBaseCell() && kv.getKey().getCell(1) == originalCell.getCell(1) && kv.getKey().getCell(2) == originalCell.getCell(2)
                    )
                    .toList();
            if (res.size() != matching.size()) {
                res = sctm.subTree(originalCell);
            }
            for (H3CellId<String> cell : res.keySet()) {
                Assert.assertEquals(originalCell.getBaseCell(), cell.getBaseCell());
                Assert.assertEquals(cell.getCell(1), originalCell.getCell(1));
                Assert.assertEquals(cell.getCell(2), originalCell.getCell(2));
            }
        }
    }

    @Test
    public void testSubtreeLeafNode() {
        final var sctm = new SpacialConcurrentTrieMap<String, Integer>();
        var originalCell = generateNonRandomCell(31, new int[] {1, 4, 3, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 3, null);
        sctm.put(originalCell, 42);

        var cellToFind = generateNonRandomCell(31, new int[] {1, 4, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 2, null);
        var cellNotFound1 = generateNonRandomCell(31, new int[] {1, 2, 3, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 3, null);
        var cellNotFound2 = generateNonRandomCell(31, new int[] {1, 4, 3, 2, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 4, null);
        var cellBaseZero = generateNonRandomCell(31, new int[] {7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 0, null);
        var cellOtherBase = generateNonRandomCell(95, new int[] {1, 4, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 2, null);

        var res = sctm.subTree(cellToFind);
        Assert.assertTrue(res.containsKey(originalCell));
        res = sctm.subTree(originalCell);
        Assert.assertTrue(res.containsKey(originalCell));
        res = sctm.subTree(cellNotFound1);
        Assert.assertFalse(res.containsKey(originalCell));
        res = sctm.subTree(cellNotFound2);
        Assert.assertFalse(res.containsKey(originalCell));
        res = sctm.subTree(cellBaseZero);
        Assert.assertTrue(res.containsKey(originalCell));
        res = sctm.subTree(cellOtherBase);
        Assert.assertFalse(res.containsKey(originalCell));
    }

    @Test
    public void testSubtreeCellCollision() {
        var originalCell = generateNonRandomCell(31, new int[] {1, 4, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7}, 2, null);
        var cell1 = generateNonRandomCell(640607404924394671L, "one");
        var cell2 = generateNonRandomCell(640607933564237935L, "two");
        var cell3 = generateNonRandomCell(640608096145901063L, "three");
        var cell4 = generateNonRandomCell(618090672307044351L, "four");
        final var sctm = new SpacialConcurrentTrieMap<String, Integer>();
        sctm.put(cell1, 1);
        sctm.put(cell2, 2);
        sctm.put(cell3, 3);
        sctm.put(cell4, 4);
        var res = sctm.subTree(originalCell);
        for (H3CellId<String> cell : res.keySet()) {
            Assert.assertEquals(originalCell.getBaseCell(), cell.getBaseCell());
            Assert.assertEquals(cell.getCell(1), originalCell.getCell(1));
            Assert.assertEquals(cell.getCell(2), originalCell.getCell(2));
        }
    }

    @Test
    public void testSubtreeWithSurrounding() throws Exception {
        var h3 = H3Core.newInstance();
        var originalCell = generateNonRandomCell(31, new int[] {0, 2, 2, 3, 4, 4, 6, 6, 3, 2, 3, 6, 2, 0, 6}, 15, "original");
        var sctm = new SpacialConcurrentTrieMap<String, String>();
        sctm.put(originalCell, "100500");

        var trimmed = H3CellId.trimToResolution("8f3e12726cd3c86", 8);
        var surrounding = h3.gridRingUnsafe(trimmed, 1); // should be 6 cells
        for (String cid : surrounding) {
            var cellId = new H3CellId<String>(cid, null);
            var sub = sctm.subTree(cellId);
            Assert.assertEquals(sub.size(), 0);
        }
    }

    @Test
    public void testSubtreeWithCommonParent() {
        // 0x853e6283fffffff
        var cell1 = new H3CellId<>("853e6283fffffff", UUID.randomUUID());
        // 0x853e6287fffffff
        var cell2 = new H3CellId<>("853e6287fffffff", UUID.randomUUID());
        // 0x853e628ffffffff
        var cell3 = new H3CellId<>("853e628ffffffff", UUID.randomUUID());

        var values = new ArrayList<H3CellId<String>>(10000);
        var rnd = new Random();
        var sctm = new SpacialConcurrentTrieMap<UUID, String>();
        for (int i = 0; i < 10000; i++) {
            var k = rnd.nextInt(3);
            H3CellId<String> cellId = null;
            switch (k) {
                case 0:
                    cellId = generateRandomChildForCell(cell1.getAddress(), 15, String.valueOf(i));
                    break;
                case 1:
                    cellId = generateRandomChildForCell(cell2.getAddress(), 15, String.valueOf(i));
                    break;
                case 2:
                    cellId = generateRandomChildForCell(cell3.getAddress(), 15, String.valueOf(i));
                    break;
            }
            values.add(cellId);
            sctm.put(new H3CellId<>(cellId.getCellId(), UUID.randomUUID()), null);
        }
        var s1 = sctm.subTree(cell1).size();
        var s2 = sctm.subTree(cell2).size();
        var s3 = sctm.subTree(cell3).size();
        Assert.assertEquals(s1 + s2 + s3, 10000);
    }
}
