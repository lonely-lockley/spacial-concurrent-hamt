package com.github.lonelylockley.spatial;

import org.github.jamm.MemoryMeter;
import org.testng.annotations.Test;

public class MeasureMemoryFootprint extends TestBase<String> {

    private void printResults(long trkTotal, int size) {
        System.out.printf("%s size is %s with %s elements%n", "LocationTracker", sizeToHumanReadable(trkTotal), size);
    }

    private long measureLocationTracker(int size, MemoryMeter mm) {
        final var trk = new LocationTracker<>();
        for (int i = 0; i < size; i++) {
            var key = String.valueOf(i);
            var cellId = generateRandomCell(key);
            trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i);
        }
        var total = mm.measureDeep(trk);
        return total;
    }

    @Test(priority = 1)
    public void zeroElements() {
        System.out.println();
        final var size = 0;
        var mm = MemoryMeter.builder().build();
        var trkTotal = measureLocationTracker(size, mm);
        printResults(trkTotal, size);
        System.out.println();
    }

    @Test(priority = 2)
    public void tenElements() {
        final var size = 10;
        var mm = MemoryMeter.builder().build();
        var trkTotal = measureLocationTracker(size, mm);
        printResults(trkTotal, size);
        System.out.println();
    }

    @Test(priority = 3)
    public void hundredElements() {
        final var size = 100;
        var mm = MemoryMeter.builder().build();
        var trkTotal = measureLocationTracker(size, mm);
        printResults(trkTotal, size);
        System.out.println();
    }

    @Test(priority = 4)
    public void thousandElements() {
        final var size = 1000;
        var mm = MemoryMeter.builder().build();
        var trkTotal = measureLocationTracker(size, mm);
        printResults(trkTotal, size);
        System.out.println();
    }

    @Test(priority = 5)
    public void tenThousandElements() {
        final var size = 10000;
        var mm = MemoryMeter.builder().build();
        var trkTotal = measureLocationTracker(size, mm);
        printResults(trkTotal, size);
        System.out.println();
    }

    @Test(priority = 6)
    public void hundredThousandElements() {
        final var size = 100000;
        var mm = MemoryMeter.builder().build();
        var trkTotal = measureLocationTracker(size, mm);
        printResults(trkTotal, size);
        System.out.println();
    }

    @Test(priority = 7)
    public void millionElements() {
        final var size = 1000000;
        var mm = MemoryMeter.builder().build();
        var trkTotal = measureLocationTracker(size, mm);
        printResults(trkTotal, size);
        System.out.println();
    }

}
