package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;
import com.github.lonelylockley.spatial.ctrie.SpatialConcurrentTrieMap;
import com.romix.scala.collection.concurrent.TrieMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Thread)
@Fork(value = 1)
@Threads(1)
public class CompareMapsPutSpeed extends TestBase<String> {

    private static final int SIZE = 50000;

    private final Random rng = new Random();

    private ArrayList<H3CellId<String>> values = new ArrayList<>(SIZE);

    private ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();
    private TrieMap<String, Integer> concurrentHamt = new TrieMap<>();
    private SpatialConcurrentTrieMap<String, Integer> concurrentSpatialHamt = new SpatialConcurrentTrieMap<>();

    @Setup(Level.Trial)
    public void setup() {
        for (int i = 0; i < SIZE; i++) {
            var key = String.valueOf(i);
            var cellId = generateRandomCell(key);
            values.add(cellId);
        }
    }

    @Setup(Level.Iteration)
    public void reset() {
        concurrentHashMap = new ConcurrentHashMap<>();
        concurrentHamt = new TrieMap<>();
        concurrentSpatialHamt = new SpatialConcurrentTrieMap<>();
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHashMapPutThroughput(Blackhole blackhole) {
        var id = rng.nextInt(SIZE);
        var v = concurrentHashMap.put(values.get(id).getCellId(), id);
        blackhole.consume(v);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHamtPutThroughput(Blackhole blackhole) {
        var id = rng.nextInt(SIZE);
        var v = concurrentHamt.put(values.get(id).getCellId(), id);
        blackhole.consume(v);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentSpatialHamtPutThroughput(Blackhole blackhole) {
        var id = rng.nextInt(SIZE);
        var v = concurrentSpatialHamt.put(values.get(id), id);
        blackhole.consume(v);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureConcurrentHashMapPutLatency(Blackhole blackhole) {
        var id = rng.nextInt(SIZE);
        var v = concurrentHashMap.put(values.get(id).getCellId(), id);
        blackhole.consume(v);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureConcurrentHamtPutLatency(Blackhole blackhole) {
        var id = rng.nextInt(SIZE);
        var v = concurrentHamt.put(values.get(id).getCellId(), id);
        blackhole.consume(v);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureConcurrentSpatialHamtPutLatency(Blackhole blackhole) {
        var id = rng.nextInt(SIZE);
        var v = concurrentSpatialHamt.put(values.get(id), id);
        blackhole.consume(v);
    }

}
