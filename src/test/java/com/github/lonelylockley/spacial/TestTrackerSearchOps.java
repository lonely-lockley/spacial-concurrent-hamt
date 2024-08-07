package com.github.lonelylockley.spacial;

import com.github.lonelylockley.spacial.ctrie.H3CellId;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

public class TestTrackerSearchOps extends TestBase<String> {

    @Test
    public void testGetAllWithingRing() {
        Tracker<String, Integer> trk = new LocationTracker<>();
        final var values = new ArrayList<H3CellId<String>>(10000);

        for (int i = 0; i < 10000; i++) {
            var cellId = generateNonRandomCellFullRes(54, String.valueOf(i));
            Assert.assertNotNull(trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i));
            values.add(cellId);
        }

    }
}
