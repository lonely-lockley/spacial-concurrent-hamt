package com.github.lonelylockley.spacial;

import com.github.lonelylockley.spacial.ctrie.SpacialConcurrentTrieMap;
import com.romix.scala.collection.concurrent.TrieMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Thread)
@Fork(value = 1)
@Threads(1)
public class CompareCopySpeed extends TestBase<String> {

    private static final int SIZE = 50000;

    private ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();
    private TrieMap<String, Integer> concurrentHamt = new TrieMap<>();
    private SpacialConcurrentTrieMap<String, Integer> concurrentSpacialHamt = new SpacialConcurrentTrieMap<>();

    @Setup(Level.Trial)
    public void setup() {
        for (int i = 0; i < SIZE; i++) {
            var key = String.valueOf(i);
            var cellId = generateRandomCell(key);
            concurrentHashMap.put(key, i);
            concurrentHamt.put(key, i);
            concurrentSpacialHamt.put(cellId, i);
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
    public void measureConcurrentSpacialHamtCopyThroughput(final Blackhole blackhole) {
        var n = concurrentSpacialHamt.snapshot();
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
    public void measureConcurrentSpacialHamtCopyLatency(final Blackhole blackhole) {
        var n = concurrentSpacialHamt.snapshot();
        blackhole.consume(n);
    }

}
