package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Benchmark)
@Fork(value = 1)
@Threads(2)
public class MeasureMoveSpeed extends TestBase<String> {

    private static final int LIMIT = 500;

    private final Random rng = new Random();

    private LocationTracker<String, Integer> trk = new LocationTracker<>();
    private String[] businessEntities = new String[LIMIT];


    private H3CellId<String> createCell(String businessEntityId) {
        var tmp = generateNonRandomCell(31, new int[] {1, 4, 2, 4, 3, 4, 2, 0, 4, 4, 2, 6, 1, 5, 5}, 5, 15, businessEntityId);
        return new H3CellId<>(H3CellId.trimToResolution(tmp.getCellId(), 15), businessEntityId);
    }

    @Setup(Level.Iteration)
    public void setup() {
        trk = new LocationTracker<>();
        businessEntities = new String[LIMIT];
        for (int i = 0; i < LIMIT; i++) {
            var cellId = createCell("businessEntity:" + i);
            trk.startTracking(cellId.getCellId(), cellId.getBusinessEntityId(), i);
            businessEntities[i] = cellId.getBusinessEntityId();
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureUpdateLocationThroughput(Blackhole blackhole) {
        var businessEntityId = businessEntities[rng.nextInt(LIMIT)];
        var res = trk.updateLocation(businessEntityId, createCell(businessEntityId));
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureFindAroundThroughput(Blackhole blackhole) {
        var res = trk.findAround(createCell("test").getCellId(), (k, v) -> true, 7, 3, 30);
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureUpdateAndFindThroughput(Blackhole blackhole) {
        var businessEntityId = businessEntities[rng.nextInt(LIMIT)];
        var res1 = trk.updateLocation(businessEntityId, createCell(businessEntityId));
        blackhole.consume(res1);
        var res2 = trk.findAround(createCell("test").getCellId(), (k, v) -> true, 7, 3, 30);
        blackhole.consume(res2);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureUpdateAndFindLatency(Blackhole blackhole) {
        var businessEntityId = businessEntities[rng.nextInt(LIMIT)];
        var res1 = trk.updateLocation(businessEntityId, createCell(businessEntityId));
        blackhole.consume(res1);
        var res2 = trk.findAround(createCell("test").getCellId(), (k, v) -> true, 7, 3, 100);
        blackhole.consume(res2);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureFindAroundLatency(Blackhole blackhole) {
        var res = trk.findAround(createCell("test").getCellId(), (k, v) -> true, 7, 3, 100);
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureUpdateLocationLatency(Blackhole blackhole) {
        var businessEntityId = businessEntities[rng.nextInt(LIMIT)];
        var res = trk.updateLocation(businessEntityId, createCell(businessEntityId));
        blackhole.consume(res);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureRandomGenerateCellLatency(Blackhole blackhole) {
        var id = rng.nextInt(LIMIT);
        var cellId = createCell(businessEntities[id]);
        blackhole.consume(cellId);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureRandomGenerateIdLatency(Blackhole blackhole) {
        var id = rng.nextInt(LIMIT);
        blackhole.consume(id);
    }

}
