package com.github.lonelylockley.spatial;

import com.github.lonelylockley.spatial.ctrie.H3CellId;
import com.github.lonelylockley.spatial.ctrie.SpatialConcurrentTrieMap;
import com.uber.h3core.H3Core;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class LocationTracker<T, V> implements Tracker<T, V> {

    private final ConcurrentHashMap<T, H3CellId<T>> businessEntityIndex = new ConcurrentHashMap<>();
    private final SpatialConcurrentTrieMap<T, V> locations = new SpatialConcurrentTrieMap<>();
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock writeLock = readWriteLock.writeLock();
    private final Lock readLock = readWriteLock.readLock();
    private H3Core h3;

    public LocationTracker() {
        try {
            h3 = H3Core.newInstance();
        }
        catch (Exception ex) {
            throw new RuntimeException("Could not initialize Uber H3", ex);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V startTracking(final String cellId, final T businessEntityId, final V value) {
        final var h3CellId = new H3CellId<>(cellId, businessEntityId);
        businessEntityIndex.computeIfAbsent(businessEntityId, (key) -> {
            locations.put(h3CellId, value);
            return h3CellId;
        });
        return value;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean updateLocation(final T businessEntityId, final H3CellId<T> toCellId) {
        final var res = businessEntityIndex.computeIfPresent(businessEntityId, (key, fromCellId) -> {
            writeLock.lock();
            {
                locations.put(toCellId, locations.remove(fromCellId));
            }
            writeLock.unlock();
            return toCellId;
        });
        return res != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V finishTracking(final T businessEntityId) {
        var cellId = businessEntityIndex.remove(businessEntityId);
        if (cellId == null) {
            return null;
        }
        else {
            return locations.remove(cellId);
        }
    }

    public void updateValue(final T businessEntityId, final V value) {
        businessEntityIndex.computeIfPresent(businessEntityId, (key, cellId) -> {
            locations.put(cellId, value);
            return cellId;
        });
    }

    @Override
    public boolean isTracking(T businessEntityId) {
        return businessEntityIndex.containsKey(businessEntityId);
    }

    @Override
    public H3CellId<T> getLocation(T businessEntityId) {
        return businessEntityIndex.get(businessEntityId);
    }

    @Override
    public V getValue(T businessEntityId) {
        var cellId = businessEntityIndex.get(businessEntityId);
        if (cellId == null) {
            return null;
        }
        else {
            return locations.get(cellId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Map.Entry<H3CellId<T>, V>> getAllWithinRing(final String cellId, final int resolution, final int range) {
        var trimmed = H3CellId.trimToResolution(cellId, resolution);
        SpatialConcurrentTrieMap<T, V> snapshot;
        readLock.lock();
        {
            snapshot = locations.readOnlySnapshot();
        }
        readLock.unlock();
        var ring = h3.gridRingUnsafe(trimmed, range);
        // @ToDo there should be a resolution when it's cheaper to get a larger cell encapsulating all desired in a single call and filter the excess entries - a good point for optimization
        return ring
                .stream()
                .flatMap(cid -> snapshot
                                    .subTree(new H3CellId<>(cid, null))
                                    .entrySet()
                                    .stream()
                                    .map((e) -> {
                                        var key = e.getKey();
                                        return (Map.Entry<H3CellId<T>, V>) new AbstractMap.SimpleImmutableEntry(key, e.getValue());
                                    })
                                    .filter(e -> e.getValue() != null)
                )
                .toList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Map.Entry<H3CellId<T>, V>> getAllWithinCircle(String cellId, int resolution, int range) {
        var trimmed = H3CellId.trimToResolution(cellId, resolution);
        SpatialConcurrentTrieMap<T, V> snapshot;
        readLock.lock();
        {
            snapshot = locations.readOnlySnapshot();
        }
        readLock.unlock();
        var circle = h3.gridDiskUnsafe(trimmed, range);
        return circle
                .stream()
                .flatMap(Collection::stream)
                .flatMap(cid -> snapshot
                        .subTree(new H3CellId<>(cid, null))
                        .entrySet()
                        .stream()
                        .map((e) -> {
                            var key = e.getKey();
                            return (Map.Entry<H3CellId<T>, V>) new AbstractMap.SimpleImmutableEntry(key, e.getValue());
                        })
                        .filter(e -> e.getValue() != null)
                )
                .toList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<T, H3CellId<T>> getAllBusinessEntitiesLocations() {
        return locations
                .keySet()
                .stream()
                .map(kv -> new AbstractMap.SimpleImmutableEntry<>(kv.getBusinessEntityId(), kv)).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (x, y) -> y));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Map.Entry<H3CellId<T>, V>> findAround(String cellId, BiFunction<H3CellId<T>, V, Boolean> predicate, int resolution, int range, int limit) {
        var trimmed = H3CellId.trimToResolution(cellId, resolution);
        SpatialConcurrentTrieMap<T, V> snapshot;
        readLock.lock();
        {
            snapshot = locations.readOnlySnapshot();
        }
        readLock.unlock();
        var result = new ArrayList<Map.Entry<H3CellId<T>, V>>(limit);
        for (int r = 0; r <= range; r++) {
            var ring = (range == 0) ? Collections.singleton(trimmed).iterator() : h3.gridRingUnsafe(trimmed, r).iterator();
            while (result.size() < limit && ring.hasNext()) {
                var nxt = ring.next();
                var ringData = snapshot.subTree(new H3CellId<>(nxt, null)).entrySet().iterator();
                while (result.size() < limit && ringData.hasNext()) {
                    var entry = ringData.next();
                    var key = entry.getKey();
                    var value = entry.getValue();
                    if (value != null && predicate.apply(key, value)) {
                        result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                    }
                }
            }
        }
        return result;
    }

}
