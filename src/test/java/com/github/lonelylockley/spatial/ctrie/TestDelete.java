package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;


public class TestDelete extends TestBase<String> {
    @Test
    public void testDelete() {
        final var sctm = new SpatialConcurrentTrieMap<String, Integer>();
        final var values = new ArrayList<H3CellId<String>>(10000);

        for (int i = 0; i < 10000; i++) {
            var cellId = generateRandomCell(String.valueOf(i));
            Assert.assertNull(sctm.put(cellId, i));
            final var lookup = sctm.lookup(cellId);
            Assert.assertEquals(i, lookup);
            values.add(cellId);
        }

        // check hash collisions
        checkAddInsert(sctm, generateNonRandomCell( 56,  "56"));   //  56 | idx= 24, flag= 16777216, bmp= -1, mask= 16777215, pos= 24
        checkAddInsert(sctm, generateNonRandomCell( 85,  "85"));   //  85 | idx= 21, flag=  2097152, bmp= -1, mask=  2097151, pos= 21
        checkAddInsert(sctm, generateNonRandomCell(117, "117"));   // 117 | idx= 21, flag=  2097152, bmp= -1, mask=  2097151, pos= 21
        
        for (int i = 0; i < 10000; i++) {
            Assert.assertNotNull(sctm.remove(values.get(i)));
            Assert.assertNull(sctm.remove(values.get(i)));
        }
    }

    private static void checkAddInsert(final SpatialConcurrentTrieMap<String, Integer> sctm, H3CellId<String> cellId) {
        sctm.remove(cellId);
        Integer foundV = sctm.lookup(cellId);
        Assert.assertNull(foundV);
        Assert.assertNull(sctm.put(cellId, cellId.getBaseCell()));
        foundV = sctm.lookup(cellId);
        Assert.assertEquals(cellId.getBaseCell(), foundV);
        Assert.assertEquals(foundV, sctm.put(cellId, -1));
        Assert.assertEquals(-1, sctm.put (cellId, cellId.getBaseCell()));
    }
}
