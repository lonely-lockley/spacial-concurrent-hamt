package com.github.lonelylockley.spacial.ctrie;

import com.github.lonelylockley.spacial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestHashCollisions extends TestBase<String> {

    @Test
    public void testHashCollisions () {
        final var sctm = new SpacialConcurrentTrieMap<String, Integer>();
        final var layers = new int[] { 3, 4, 1, 3, 0, 2, 6, 4, 6, 5, 5, 2, 4, 5, 5 };

        // AaAaAa.hashCode() = 1952508096
        // AaAaBB.hashCode() = 1952508096
        // BBAaBB.hashCode() = 1952508096
        // BBBBBB.hashCode() = 1952508096

        //  85 | idx= 21, flag=  2097152, bmp= -1, mask=  2097151, pos= 21
        // 117 | idx= 21, flag=  2097152, bmp= -1, mask=  2097151, pos= 21
        //  56 | idx= 24, flag= 16777216, bmp= -1, mask= 16777215, pos= 24
        final var cell1 = generateNonRandomCell( 85, layers, 15, "AaAaAa");
        final var cell2 = generateNonRandomCell( 85, layers, 15, "AaAaBB");
        final var cell3 = generateNonRandomCell(117, layers, 15, "BBAaBB");
        final var cell4 = generateNonRandomCell( 56, layers, 15, "BBBBBB");

        Assert.assertNull(sctm.put(cell4, 4));
        Assert.assertNull(sctm.put(cell2, 2));
        Assert.assertNull(sctm.put(cell1, 1));
        Assert.assertNull(sctm.put(cell3, 3));

        Assert.assertEquals(4, sctm.size());

        Assert.assertEquals(1, sctm.put(cell1, 11));
        Assert.assertEquals(2, sctm.put(cell2, 22));
        Assert.assertEquals(3, sctm.put(cell3, 33));
        Assert.assertEquals(4, sctm.put(cell4, 44));

        Assert.assertEquals(4, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertTrue(sctm.containsKey(cell4));
        Assert.assertTrue(sctm.containsKey(cell3));
        Assert.assertTrue(sctm.containsKey(cell2));
        Assert.assertTrue(sctm.containsKey(cell1));

        Assert.assertEquals(22, sctm.remove(cell2));
        Assert.assertEquals(11, sctm.remove(cell1));
        Assert.assertEquals(44, sctm.remove(cell4));
        Assert.assertEquals(33, sctm.remove(cell3));

        Assert.assertEquals(0, sctm.size());
        Assert.assertTrue(sctm.isEmpty());

        Assert.assertNull(sctm.put(cell1, 1));
        Assert.assertNull(sctm.put(cell2, 2));
        Assert.assertNull(sctm.put(cell3, 3));
        Assert.assertNull(sctm.put(cell4, 4));

        Assert.assertEquals(4, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertTrue(sctm.containsKey(cell4));
        Assert.assertTrue(sctm.containsKey(cell3));
        Assert.assertTrue(sctm.containsKey(cell2));
        Assert.assertTrue(sctm.containsKey(cell1));

        Assert.assertFalse(sctm.remove(cell2, 22));
        Assert.assertEquals(4, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertTrue(sctm.remove(cell2, 2));
        Assert.assertEquals(3, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertFalse(sctm.remove(cell1, 11));
        Assert.assertEquals(3, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertTrue(sctm.remove(cell1, 1));
        Assert.assertEquals(2, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertFalse(sctm.remove(cell4, 44));
        Assert.assertEquals(2, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertTrue(sctm.remove(cell4, 4));
        Assert.assertEquals(1, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertFalse(sctm.remove(cell3, 33));
        Assert.assertEquals(1, sctm.size());
        Assert.assertFalse(sctm.isEmpty());

        Assert.assertTrue(sctm.remove(cell3, 3));
        Assert.assertEquals(0, sctm.size());
        Assert.assertTrue(sctm.isEmpty());
    }

}
