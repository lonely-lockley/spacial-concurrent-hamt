package com.github.lonelylockley.spacial;

import com.github.lonelylockley.spacial.ctrie.H3CellId;
import com.github.lonelylockley.spacial.ctrie.SpacialConcurrentTrieMap;
import com.uber.h3core.H3Core;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class LocationTracker<T, V> implements Tracker<T, V> {

    private final ConcurrentHashMap<T, LocationWrapper<T, V>> businessEntityIndex = new ConcurrentHashMap<>();
    private final SpacialConcurrentTrieMap<T, LocationWrapper<T, V>> locations = new SpacialConcurrentTrieMap<>();
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
    public V setLocation(final String cellId, final T businessEntityId, final V value) {
        final var h3CellId = new H3CellId<>(cellId, businessEntityId);
        final var res = businessEntityIndex.computeIfAbsent(businessEntityId, (key) -> {
                    var wrapped = new LocationWrapper<>(h3CellId, value);
                    locations.put(h3CellId, wrapped);
                    return wrapped;
                });
        if (res == null) {
            return null;
        }
        else {
            return res.unwrap(h3CellId);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveLocation(final T businessEntityId, final H3CellId<T> toCellId) {
        final var res = businessEntityIndex.computeIfPresent(businessEntityId, (key, wrapper) -> {
            final var fromCellId = wrapper.unwrap();
            locations.put(toCellId, wrapper);
            wrapper.swapCellId(fromCellId, toCellId);
            locations.remove(fromCellId);
            return wrapper;
        });
        return res != null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public V removeLocation(final T businessEntityId) {
        var wrapper = businessEntityIndex.remove(businessEntityId);
        if (wrapper == null) {
            return null;
        }
        else {
            var cellId = wrapper.unwrap();
            return locations.remove(cellId).unwrap(cellId);
        }
    }

    public void updateValue(final T businessEntityId, final V value) {
        businessEntityIndex.computeIfPresent(businessEntityId, (key, wrapper) -> {
            wrapper.update(value);
            return wrapper;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Map.Entry<H3CellId<T>, V>> getAllWithinRing(final String cellId, final int resolution, final int range) {
        var trimmed = H3CellId.trimToResolution(cellId, resolution);
        var snapshot = locations.readOnlySnapshot();
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
                                        return (Map.Entry<H3CellId<T>, V>) new AbstractMap.SimpleImmutableEntry(key, e.getValue().unwrap(key));
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
        var snapshot = locations.readOnlySnapshot();
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
                            return (Map.Entry<H3CellId<T>, V>) new AbstractMap.SimpleImmutableEntry(key, e.getValue().unwrap(key));
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
        var snapshot = locations.readOnlySnapshot();
        var result = new ArrayList<Map.Entry<H3CellId<T>, V>>(limit);
        for (int r = 0; r < range; r++) {
            var ring = h3.gridRingUnsafe(trimmed, range).iterator();
            while (result.size() < limit && ring.hasNext()) {
                var ringData = snapshot.subTree(new H3CellId<>(ring.next(), null)).entrySet().iterator();
                while (result.size() < limit && ringData.hasNext()) {
                    var entry = ringData.next();
                    var key = entry.getKey();
                    var value = entry.getValue().unwrap(entry.getKey());
                    if (value != null && predicate.apply(key, value)) {
                        result.add(new AbstractMap.SimpleImmutableEntry(key, value));
                    }
                }
            }
        }
        return result;
    }

}
