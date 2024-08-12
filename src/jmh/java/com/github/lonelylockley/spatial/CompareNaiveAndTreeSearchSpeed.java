package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;
import com.uber.h3core.H3Core;
import com.uber.h3core.LengthUnit;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Thread)
@Fork(value = 1)
@Threads(1)
public class CompareNaiveAndTreeSearchSpeed extends TestBase<UUID> {

    private static final int SIZE = 10000;

    private H3Core h3;
    private Random rnd = new Random();

    private List<Map.Entry<H3CellId<UUID>, String>> values = new ArrayList<>(SIZE);

    // choose ConcurrentHashMap as the fastest of all collections
    private ConcurrentHashMap<String, List<H3CellId<UUID>>> concurrentHashMap = new ConcurrentHashMap<>();
    private LocationTracker<UUID, Integer> concurrentSpatialHamt = new LocationTracker<>();

    @Setup(Level.Trial)
    public void setup() throws Exception {
        h3 = H3Core.newInstance();
        var cell1 = new H3CellId<>("853e6283fffffff", null);
        var cell2 = new H3CellId<>("853e6287fffffff", null);
        var cell3 = new H3CellId<>("853e628ffffffff", null);
        var rnd = new Random();
        for (int i = 0; i < SIZE; i++) {
            var c = rnd.nextInt(3);
            H3CellId<UUID> cellId = null;
            switch (c) {
                case 0:
                    var cid1 = generateRandomChildForCell(cell1.getAddress(), 15, UUID.randomUUID());
                    cellId = cid1;
                    concurrentHashMap.compute(cell1.getCellId(), (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>();
                        }
                        v.add(cid1);
                        return v;
                    });
                    values.add(new AbstractMap.SimpleEntry<>(cid1, cell1.getCellId()));
                    break;
                case 1:
                    var cid2 = generateRandomChildForCell(cell2.getAddress(), 15, UUID.randomUUID());
                    cellId = cid2;
                    concurrentHashMap.compute(cell2.getCellId(), (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>();
                        }
                        v.add(cid2);
                        return v;
                    });
                    values.add(new AbstractMap.SimpleEntry<>(cid2, cell2.getCellId()));
                    break;
                case 2:
                    var cid3 = generateRandomChildForCell(cell3.getAddress(), 15, UUID.randomUUID());
                    cellId = cid3;
                    concurrentHashMap.compute(cell3.getCellId(), (k, v) -> {
                        if (v == null) {
                            v = new ArrayList<>();
                        }
                        v.add(cid3);
                        return v;
                    });
                    values.add(new AbstractMap.SimpleEntry<>(cid3, cell3.getCellId()));
                    break;
            }
            concurrentSpatialHamt.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureNaiveSearchThroughput(Blackhole blackhole) {
        var base = values.get(rnd.nextInt(SIZE));
        var entries = concurrentHashMap.get(base.getValue());
        var res = entries
                .stream()
                .map(c -> new AbstractMap.SimpleEntry<>(c, h3.greatCircleDistance(h3.cellToLatLng(base.getKey().getAddress()), h3.cellToLatLng(c.getAddress()), LengthUnit.m)))
                .sorted(Comparator.comparingDouble(AbstractMap.SimpleEntry::getValue))
                .limit(20)
                .toList();
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureTreeSearchThroughput(Blackhole blackhole) {
        var base = values.get(rnd.nextInt(SIZE));
        var res = concurrentSpatialHamt.findAround(base.getKey().getCellId(), (k, v) -> true, 8, 3, 20);
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureNaiveSearchLatency(Blackhole blackhole) {
        var base = values.get(rnd.nextInt(SIZE));
        var entries = concurrentHashMap.get(base.getValue());
        var res = entries
                .stream()
                .map(c -> new AbstractMap.SimpleEntry<>(c, h3.greatCircleDistance(h3.cellToLatLng(base.getKey().getAddress()), h3.cellToLatLng(c.getAddress()), LengthUnit.m)))
                .sorted(Comparator.comparingDouble(AbstractMap.SimpleEntry::getValue))
                .limit(20)
                .toList();
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureTreeSearchLatency(Blackhole blackhole) {
        var base = values.get(rnd.nextInt(SIZE));
        var res = concurrentSpatialHamt.findAround(base.getKey().getCellId(), (k, v) -> true, 8, 3, 20);
        blackhole.consume(res);
    }

}
