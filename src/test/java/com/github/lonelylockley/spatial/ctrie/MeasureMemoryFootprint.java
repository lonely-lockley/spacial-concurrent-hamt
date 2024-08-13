package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.TestBase;
import com.romix.scala.collection.concurrent.TrieMap;
import org.github.jamm.MemoryMeter;
import org.testng.annotations.Test;

import java.util.concurrent.ConcurrentHashMap;

public class MeasureMemoryFootprint extends TestBase<String> {

    private void printResults(long chmTotal, long chTotal, long cshTotal, int size) {
        System.out.printf("%s size is %s with %s elements%n", "concurrentHashMap", sizeToHumanReadable(chmTotal), size);
        System.out.printf("%s size is %s with %s elements%n", "concurrentHamt", sizeToHumanReadable(chTotal), size);
        System.out.printf("%s size is %s with %s elements%n", "concurrentSpatialHamt", sizeToHumanReadable(cshTotal), size);
    }

    private long measureConcurrentHashMap(int size, MemoryMeter mm) {
        final var concurrentHashMap = new ConcurrentHashMap<String, Integer>();
        for (int i = 0; i < size; i++) {
            var key = String.valueOf(i);
            concurrentHashMap.put(key, i);
        }
        var total = mm.measureDeep(concurrentHashMap);
        return total;
    }

    private long measureConcurrentHamt(int size, MemoryMeter mm) {
        var concurrentHamt = new TrieMap<String, Integer>();
        for (int i = 0; i < size; i++) {
            var key = String.valueOf(i);
            concurrentHamt.put(key, i);
        }
        var total = mm.measureDeep(concurrentHamt);
        return total;
    }

    private long measureConcurrentSpatialHamt(int size, MemoryMeter mm) {
        final var concurrentSpatialHamt = new SpatialConcurrentTrieMap<String, Integer>();
        for (int i = 0; i < size; i++) {
            var key = String.valueOf(i);
            var cellId = generateRandomCell(key);
            concurrentSpatialHamt.put(cellId, i);
        }
        var total = mm.measureDeep(concurrentSpatialHamt);
        return total;
    }

    @Test(priority = 1)
    public void zeroElements() {
        System.out.println();
        final var size = 0;
        var mm = MemoryMeter.builder().build();
        var chmTotal = measureConcurrentHashMap(size, mm);
        var chTotal = measureConcurrentHamt(size, mm);
        var cshTotal = measureConcurrentSpatialHamt(size, mm);
        printResults(chmTotal, chTotal, cshTotal, size);
        System.out.println();
    }

    @Test(priority = 2)
    public void thousandElements() {
        final var size = 1000;
        var mm = MemoryMeter.builder().build();
        var chmTotal = measureConcurrentHashMap(size, mm);
        var chTotal = measureConcurrentHamt(size, mm);
        var cshTotal = measureConcurrentSpatialHamt(size, mm);
        printResults(chmTotal, chTotal, cshTotal, size);
        System.out.println();
    }

    @Test(priority = 3)
    public void hundredThousandElements() {
        final var size = 100000;
        var mm = MemoryMeter.builder().build();
        var chmTotal = measureConcurrentHashMap(size, mm);
        var chTotal = measureConcurrentHamt(size, mm);
        var cshTotal = measureConcurrentSpatialHamt(size, mm);
        printResults(chmTotal, chTotal, cshTotal, size);
        System.out.println();
    }

    @Test(priority = 4)
    public void millionElements() {
        final var size = 1000000;
        var mm = MemoryMeter.builder().build();
        var chmTotal = measureConcurrentHashMap(size, mm);
        var chTotal = measureConcurrentHamt(size, mm);
        var cshTotal = measureConcurrentSpatialHamt(size, mm);
        printResults(chmTotal, chTotal, cshTotal, size);
        System.out.println();
    }

}
