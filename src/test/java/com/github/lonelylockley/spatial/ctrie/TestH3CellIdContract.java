package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestH3CellIdContract extends TestBase<String> {

    @Test
    public void testEqualsAndHashCode() {
        final var layers = new int[] { 3, 4, 1, 3, 0, 2, 6, 4, 6, 5, 5, 2, 4, 5, 5 };

        // AaAaAa.hashCode() = 1952508096
        // AaAaBB.hashCode() = 1952508096
        // BBAaBB.hashCode() = 1952508096
        // BBBBBB.hashCode() = 1952508096
        final var cell1 = generateNonRandomCell( 85, layers, 15, "AaAaAa");
        final var cell2 = generateNonRandomCell( 85, layers, 15, "AaAaAa");
        final var cell3 = generateNonRandomCell(85, layers, 15, "BBAaBB");
        final var cell4 = generateNonRandomCell( 17, layers, 15, "BBBBBB");

        Assert.assertEquals(cell1, cell2);
        Assert.assertNotEquals(cell1, cell3);
        Assert.assertNotEquals(cell1, cell4);
        Assert.assertNotEquals(cell2, cell3);
        Assert.assertNotEquals(cell2, cell4);
        Assert.assertNotEquals(cell3, cell4);
    }
}
