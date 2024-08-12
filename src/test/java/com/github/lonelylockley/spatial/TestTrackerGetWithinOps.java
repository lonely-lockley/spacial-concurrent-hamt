package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;

public class TestTrackerGetWithinOps extends TestBase<String> {

    // res 7 center 873e628e2ffffff (8f3e628e2881400)
    private final H3CellId<String> center = new H3CellId<>("8f3e628e2881400", "00");
    private final ArrayList<H3CellId<String>> ring0 = new ArrayList<>();
    private final ArrayList<H3CellId<String>> ring1 = new ArrayList<>();
    private final ArrayList<H3CellId<String>> ring2 = new ArrayList<>();
    private final Tracker<String, Integer> trk = new LocationTracker<>();

    @BeforeClass
    public void setup() {
        // ring 0
        ring0.add(new H3CellId<>("8f3e628e2795b83", "01"));
        ring0.add(new H3CellId<>("8f3e628e239aa30", "02"));
        ring0.add(new H3CellId<>("8f3e628e299e10c", "03"));
        // ring 1
        ring1.add(new H3CellId<>("8f3e628e60e44e4", "04"));
        ring1.add(new H3CellId<>("8f3e628e07a6853", "05"));
        ring1.add(new H3CellId<>("8f3e628e3133af2", "06"));
        ring1.add(new H3CellId<>("8f3e628c4404100", "07"));
        ring1.add(new H3CellId<>("8f3e628f5c9e500", "08"));
        ring1.add(new H3CellId<>("8f3e628f15008c2", "09"));
        // ring 2
        ring2.add(new H3CellId<>("8f3e6281b1a9400", "10"));
        ring2.add(new H3CellId<>("8f3e628194e12a0", "11"));
        ring2.add(new H3CellId<>("8f3e628e430ca53", "12"));
        ring2.add(new H3CellId<>("8f3e628e506c464", "13"));
        ring2.add(new H3CellId<>("8f3e628e10dd4a0", "14"));
        ring2.add(new H3CellId<>("8f3e628ee2adcf0", "15"));
        ring2.add(new H3CellId<>("8f3e628c5ba371e", "16"));
        ring2.add(new H3CellId<>("8f3e628c039c846", "17"));
        ring2.add(new H3CellId<>("8f3e628c64184a2", "18"));
        ring2.add(new H3CellId<>("8f3e628f3195013", "19"));
        ring2.add(new H3CellId<>("8f3e628f0cc6cc4", "20"));
        ring2.add(new H3CellId<>("8f3e628f0088531", "21"));

        for (H3CellId<String> cellId : ring0) {
            trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), 0);
        }
        for (H3CellId<String> cellId : ring1) {
            trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), 1);
        }
        System.err.println();
        for (H3CellId<String> cellId : ring2) {
            trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), 2);
        }
    }

    @Test
    public void testGetAllWithinCircle() {
        var res = trk.getAllWithinCircle(center.getCellId(), 7, 0);
        Assert.assertEquals(res.size(), 3);
        var set = new HashSet<>(ring0);
        for (Map.Entry<H3CellId<String>, Integer> entry : res) {
            Assert.assertEquals(entry.getValue(), 0);
            Assert.assertTrue(set.remove(entry.getKey()));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testGetAllWithinCircleInRadius1() {
        var res = trk.getAllWithinCircle(center.getCellId(), 7, 1);
        Assert.assertEquals(res.size(), 9);
        var set = new HashSet<>(ring0);
        set.addAll(ring1);
        for (Map.Entry<H3CellId<String>, Integer> entry : res) {
            Assert.assertTrue(entry.getValue() >= 0 && entry.getValue() < 2);
            Assert.assertTrue(set.remove(entry.getKey()));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testGetAllWithinCircleInRadius2() {
        var res = trk.getAllWithinCircle(center.getCellId(), 7, 2);
        Assert.assertEquals(res.size(), 21);
        var set = new HashSet<>(ring0);
        set.addAll(ring1);
        set.addAll(ring2);
        for (Map.Entry<H3CellId<String>, Integer> entry : res) {
            Assert.assertTrue(entry.getValue() >= 0 && entry.getValue() <= 2);
            Assert.assertTrue(set.remove(entry.getKey()));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testGetAllWithinSingleCell() {
        var res = trk.getAllWithinRing(center.getCellId(), 7, 0);
        Assert.assertEquals(res.size(), 3);
        var set = new HashSet<>(ring0);
        for (Map.Entry<H3CellId<String>, Integer> entry : res) {
            Assert.assertEquals(entry.getValue(), 0);
            Assert.assertTrue(set.remove(entry.getKey()));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testGetAllWithinRingInRadius1() {
        var res = trk.getAllWithinRing(center.getCellId(), 7, 1);
        Assert.assertEquals(res.size(), 6);
        var set = new HashSet<>(ring1);
        for (Map.Entry<H3CellId<String>, Integer> entry : res) {
            Assert.assertEquals(entry.getValue(), 1);
            Assert.assertTrue(set.remove(entry.getKey()));
        }
        Assert.assertEquals(0, set.size());
    }

    @Test
    public void testGetAllWithinRingInRadius2() {
        var res = trk.getAllWithinRing(center.getCellId(), 7, 2);
        Assert.assertEquals(res.size(), 12);
        var set = new HashSet<>(ring2);
        for (Map.Entry<H3CellId<String>, Integer> entry : res) {
            Assert.assertEquals(entry.getValue(), 2);
            Assert.assertTrue(set.remove(entry.getKey()));
        }
        Assert.assertEquals(0, set.size());
    }
}
