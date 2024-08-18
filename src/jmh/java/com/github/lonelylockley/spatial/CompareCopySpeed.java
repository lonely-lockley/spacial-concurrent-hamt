package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.SpatialConcurrentTrieMap;
import com.romix.scala.collection.concurrent.TrieMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Thread)
@Fork(value = 1, jvmArgs = {"-Xms1G", "-Xmx1G", "-XX:ActiveProcessorCount=1"})
@Threads(1)
public class CompareCopySpeed extends TestBase<String> {

    private static final int SIZE = 50000;

    private ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();
    private TrieMap<String, Integer> concurrentHamt = new TrieMap<>();
    private SpatialConcurrentTrieMap<String, Integer> concurrentSpatialHamt = new SpatialConcurrentTrieMap<>();

    @Setup(Level.Trial)
    public void setup() {
        for (int i = 0; i < SIZE; i++) {
            var key = String.valueOf(i);
            var cellId = generateRandomCell(key);
            concurrentHashMap.put(key, i);
            concurrentHamt.put(key, i);
            concurrentSpatialHamt.put(cellId, i);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHashMapCopyThroughput(final Blackhole blackhole) {
        var n = new ConcurrentHashMap<>(concurrentHashMap);
        blackhole.consume(n);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHamtCopyThroughput(final Blackhole blackhole) {
        var n = concurrentHamt.snapshot();
        blackhole.consume(n);
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentSpatialHamtCopyThroughput(final Blackhole blackhole) {
        var n = concurrentSpatialHamt.snapshot();
        blackhole.consume(n);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureConcurrentHashMapCopyLatency(final Blackhole blackhole) {
        var n = new ConcurrentHashMap<>(concurrentHashMap);
        blackhole.consume(n);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureConcurrentHamtCopyLatency(final Blackhole blackhole) {
        var n = concurrentHamt.snapshot();
        blackhole.consume(n);
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MICROSECONDS)
    public void measureConcurrentSpatialHamtCopyLatency(final Blackhole blackhole) {
        var n = concurrentSpatialHamt.snapshot();
        blackhole.consume(n);
    }

}
