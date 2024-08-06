package com.github.lonelylockley.spacial;

import com.github.lonelylockley.spacial.ctrie.H3CellId;
import com.github.lonelylockley.spacial.ctrie.SpacialConcurrentTrieMap;
import com.romix.scala.collection.concurrent.TrieMap;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@State(value = Scope.Thread)
@Fork(value = 1)
@Threads(1)
public class CompareMapsPutSpeed extends TestBase<String> {

    private static final int SIZE = 50000;

    private Map<H3CellId<String>, Integer> values = new HashMap<>();

    private ConcurrentHashMap<String, Integer> concurrentHashMap = new ConcurrentHashMap<>();
    private TrieMap<String, Integer> concurrentHamt = new TrieMap<>();
    private SpacialConcurrentTrieMap<String, Integer> concurrentSpacialHamt = new SpacialConcurrentTrieMap<>();

    @Setup(Level.Trial)
    public void setup() {
        for (int i = 0; i < SIZE; i++) {
            var key = String.valueOf(i);
            var cellId = generateRandomCell(key);
            values.put(cellId, i);
        }
    }

    @Setup(Level.Iteration)
    public void reset() {
        concurrentHashMap = new ConcurrentHashMap<>();
        concurrentHamt = new TrieMap<>();
        concurrentSpacialHamt = new SpacialConcurrentTrieMap<>();
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHashMapPutThroughput(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHashMap.put(entry.getKey().getBusinessEntityId(), entry.getValue());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHamtPutThroughput(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHamt.put(entry.getKey().getBusinessEntityId(), entry.getValue());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentSpacialHamtPutThroughput(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentSpacialHamt.put(entry.getKey(), entry.getValue());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureConcurrentHashMapPutLatency(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHashMap.put(entry.getKey().getBusinessEntityId(), entry.getValue());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureConcurrentHamtPutLatency(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHamt.put(entry.getKey().getBusinessEntityId(), entry.getValue());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureConcurrentSpacialHamtPutLatency(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentSpacialHamt.put(entry.getKey(), entry.getValue());
            blackhole.consume(v);
        }
    }

}
