package com.github.lonelylockley.spacial;

import com.github.lonelylockley.spacial.ctrie.H3CellId;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class TestTrackerBasicOps extends TestBase<String> {

    @Test
    public void testStartFinishTrackingSingle() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        var cellId = generateNonRandomCellFullRes(91, "test");
        Assert.assertNotNull(trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), 123));
        Assert.assertEquals(trk.finishTracking(cellId.getBusinessEntityId()), 123);
    }

    @Test
    public void testStartFinishTrackingMultiple() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        final var values = new ArrayList<H3CellId<String>>(10000);

        for (int i = 0; i < 10000; i++) {
            var cellId = generateNonRandomCellFullRes(91, String.valueOf(i));
            Assert.assertNotNull(trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i));
            values.add(cellId);
        }
        for (H3CellId<String> cellId : values) {
            Assert.assertEquals(trk.finishTracking(cellId.getBusinessEntityId()), Integer.valueOf(cellId.getBusinessEntityId()));
        }
    }

    @Test
    public void testUpdateValues() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        final var values = new ArrayList<H3CellId<String>>(10000);

        for (int i = 0; i < 10000; i++) {
            var cellId = generateNonRandomCellFullRes(120, String.valueOf(i));
            Assert.assertNotNull(trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i));
            values.add(cellId);
        }
        for (H3CellId<String> cellId : values) {
            trk.updateValue(cellId.getBusinessEntityId(), Integer.parseInt(cellId.getBusinessEntityId()) + 3);
        }
        for (H3CellId<String> cellId : values) {
            Assert.assertEquals(trk.finishTracking(cellId.getBusinessEntityId()), Integer.valueOf(cellId.getBusinessEntityId()) + 3);
        }
    }

    @Test
    public void testUpdateLocation() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        final var values = new ArrayList<H3CellId<String>>(10000);
        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 100; j++) {
                var cellId = generateNonRandomCellFullRes(71, String.valueOf(i));
                if ((i * 100 + j) % 100d == 0) {
                    Assert.assertNotNull(trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i));
                }
                values.add(cellId);
            }
        }
        for (int i = 0; i < 10000; i += 100) {
            var from = values.get(i);
            for (int j = i + 1; j < i + 99; j++) {
                var to = values.get(j);
                Assert.assertTrue(trk.updateLocation(from.getBusinessEntityId(), to));
                from = to;
            }
        }
        for (int i = 100; i <= 10000; i += 100) {
            var last = values.get(i - 1);
            Assert.assertEquals(trk.finishTracking(last.getBusinessEntityId()), i / 100 - 1);
        }
    }

    @Test
    public void testIsTracking() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        var tracked = generateRandomCell("test");
        Assert.assertFalse(trk.isTracking(tracked.getBusinessEntityId()));
        trk.startTracking(tracked.getCellId(), tracked.getBusinessEntityId(), null);
        Assert.assertTrue(trk.isTracking(tracked.getBusinessEntityId()));
        var newLocation = generateRandomCell("test");
        trk.updateLocation(tracked.getBusinessEntityId(), newLocation);
        Assert.assertTrue(trk.isTracking(tracked.getBusinessEntityId()));
        trk.finishTracking(newLocation.getBusinessEntityId());
        Assert.assertFalse(trk.isTracking(tracked.getBusinessEntityId()));
    }

    @Test
    public void testGetLocation() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        var tracked = generateRandomCell("test");
        Assert.assertNull(trk.getLocation(tracked.getBusinessEntityId()));
        trk.startTracking(tracked.getCellId(), tracked.getBusinessEntityId(), 42);
        Assert.assertEquals(trk.getLocation(tracked.getBusinessEntityId()), tracked);
        var newLocation = generateRandomCell("test");
        trk.updateLocation(tracked.getBusinessEntityId(), newLocation);
        Assert.assertNotEquals(trk.getLocation(tracked.getBusinessEntityId()), tracked);
        Assert.assertEquals(trk.getLocation(tracked.getBusinessEntityId()), newLocation);
        trk.finishTracking(newLocation.getBusinessEntityId());
        Assert.assertNull(trk.getLocation(tracked.getBusinessEntityId()));
    }

    @Test
    public void testGetValue() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        var tracked = generateRandomCell("test");
        Assert.assertNull(trk.getValue(tracked.getBusinessEntityId()));
        trk.startTracking(tracked.getCellId(), tracked.getBusinessEntityId(), 42);
        Assert.assertEquals(trk.getValue(tracked.getBusinessEntityId()), 42);
        var newLocation = generateRandomCell("test");
        trk.updateLocation(tracked.getBusinessEntityId(), newLocation);
        Assert.assertEquals(trk.getValue(tracked.getBusinessEntityId()), 42);
        trk.finishTracking(newLocation.getBusinessEntityId());
        Assert.assertNull(trk.getValue(tracked.getBusinessEntityId()));
    }

}
