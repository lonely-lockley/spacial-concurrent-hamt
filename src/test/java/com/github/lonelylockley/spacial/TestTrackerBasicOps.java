package com.github.lonelylockley.spacial;

import com.github.lonelylockley.spacial.ctrie.H3CellId;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class TestTrackerBasicOps extends TestBase<String> {

    @Test
    public void testSetRemoveSingleLocation() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        var cellId = generateNonRandomCellFullRes(91, "test");
        Assert.assertNotNull(trk.setLocation(cellId.getCellId(), cellId.getBusinessEntityId(), 123));
        Assert.assertEquals(trk.removeLocation(cellId.getBusinessEntityId()), 123);
    }

    @Test
    public void testSetRemoveMultipleLocations() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        final var values = new ArrayList<H3CellId<String>>(10000);

        for (int i = 0; i < 10000; i++) {
            var cellId = generateNonRandomCellFullRes(91, String.valueOf(i));
            Assert.assertNotNull(trk.setLocation(cellId.getCellId(), cellId.getBusinessEntityId(), i));
            values.add(cellId);
        }
        for (H3CellId<String> cellId : values) {
            Assert.assertEquals(trk.removeLocation(cellId.getBusinessEntityId()), Integer.valueOf(cellId.getBusinessEntityId()));
        }
    }

    @Test
    public void testUpdateValues() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        final var values = new ArrayList<H3CellId<String>>(10000);

        for (int i = 0; i < 10000; i++) {
            var cellId = generateNonRandomCellFullRes(120, String.valueOf(i));
            Assert.assertNotNull(trk.setLocation(cellId.getCellId(), cellId.getBusinessEntityId(), i));
            values.add(cellId);
        }
        for (H3CellId<String> cellId : values) {
            trk.updateValue(cellId.getBusinessEntityId(), Integer.parseInt(cellId.getBusinessEntityId()) + 3);
        }
        for (H3CellId<String> cellId : values) {
            Assert.assertEquals(trk.removeLocation(cellId.getBusinessEntityId()), Integer.valueOf(cellId.getBusinessEntityId()) + 3);
        }
    }

    @Test
    public void testMoveLocation() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        final var values = new ArrayList<H3CellId<String>>(10000);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                var cellId = generateNonRandomCellFullRes(71, String.valueOf(i));
                if ((i * 100 + j) % 100d == 0) {
                    Assert.assertNotNull(trk.setLocation(cellId.getCellId(), cellId.getBusinessEntityId(), i));
                }
                values.add(cellId);
            }
        }
        for (int i = 0; i < 10000; i += 100) {
            var from = values.get(i);
            for (int j = i + 1; j < i + 99; j++) {
                var to = values.get(j);
                Assert.assertTrue(trk.moveLocation(from.getBusinessEntityId(), to));
                from = to;
            }
        }
        for (int i = 100; i <= 10000; i += 100) {
            var last = values.get(i - 1);
            Assert.assertEquals(trk.removeLocation(last.getBusinessEntityId()), i / 100 - 1);
        }
    }


}
