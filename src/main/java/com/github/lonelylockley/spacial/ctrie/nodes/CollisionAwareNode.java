package com.github.lonelylockley.spacial.ctrie.nodes;

import com.github.lonelylockley.spacial.ctrie.SpacialConcurrentTrieMap;
import com.github.lonelylockley.spacial.ctrie.H3CellId;
import com.github.lonelylockley.spacial.ctrie.ListMap;

import java.util.Map;
import java.util.Optional;

public class CollisionAwareNode<T, V> extends BaseNode<T, V> {

    public final int res;
    public final ListMap<H3CellId<T>, V> listmap;

    public CollisionAwareNode(final ListMap<H3CellId<T>, V> listmap, final int resolution) {
        this.listmap = listmap;
        this.res = resolution;
    }

    public CollisionAwareNode(H3CellId<T> k, V v, final int resolution) {
        this(ListMap.map(k, v), resolution);
    }

    public CollisionAwareNode(H3CellId<T> k1, V v1, H3CellId<T> k2, V v2, final int resolution) {
        this(ListMap.map(k1, v1, k2, v2), resolution);
    }

    CollisionAwareNode<T, V> inserted(H3CellId<T> k, V v) {
        return new CollisionAwareNode<>(listmap.add(k, v), res);
    }

    BaseNode<T, V> removed(H3CellId<T> k, final SpacialConcurrentTrieMap<T, V> ct) {
        ListMap<H3CellId<T>, V> updmap = listmap.remove(k);
        if (updmap.size() > 1) {
            return new CollisionAwareNode<>(updmap, res);
        }
        else {
            Map.Entry<H3CellId<T>, V> kv = updmap.iterator().next();
            // create it tombed so that it gets compressed on subsequent accesses
            return new TombstoneNode<>(kv.getKey(), kv.getValue(), kv.getKey().getAddress(), res);
        }
    }

    Optional<V> get(H3CellId<T> k) {
        return listmap.get(k);
    }

    @Override
    public int cachedSize(SpacialConcurrentTrieMap<T, V> instance) {
        return listmap.size();
    }

    @Override
    public int resolution() {
        return 0;
    }
}
