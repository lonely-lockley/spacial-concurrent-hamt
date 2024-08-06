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
public class CompareMapsGetSpeed extends TestBase<String> {

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
            concurrentHashMap.put(key, i);
            concurrentHamt.put(key, i);
            concurrentSpacialHamt.put(cellId, i);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHashMapGetThroughput(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHashMap.get(entry.getKey().getBusinessEntityId());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentHamtGetThroughput(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHamt.get(entry.getKey().getBusinessEntityId());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.Throughput})
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureConcurrentSpacialHamtGetThroughput(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentSpacialHamt.get(entry.getKey());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureConcurrentHashMapGetLatency(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHashMap.get(entry.getKey().getBusinessEntityId());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureConcurrentHamtGetLatency(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentHamt.get(entry.getKey().getBusinessEntityId());
            blackhole.consume(v);
        }
    }

    @Benchmark
    @BenchmarkMode({Mode.AverageTime})
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureConcurrentSpacialHamtGetLatency(Blackhole blackhole) {
        for (Map.Entry<H3CellId<String>, Integer> entry : values.entrySet()) {
            var v = concurrentSpacialHamt.get(entry.getKey());
            blackhole.consume(v);
        }
    }

}
