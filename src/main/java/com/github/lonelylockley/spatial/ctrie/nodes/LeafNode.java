package com.github.lonelylockley.spatial.ctrie.nodes;

import com.github.lonelylockley.spatial.ctrie.SpatialConcurrentTrieMap;
import com.github.lonelylockley.spatial.ctrie.H3CellId;

import java.util.AbstractMap;
import java.util.Map;

public class LeafNode<T, V> extends TerminalNode<T, V> {

    protected final H3CellId<T> key;
    protected final V value;
    protected final long hash;
    protected final int res;

    LeafNode(final H3CellId<T> key, final V value, final long hash, final int resolution) {
        this.key = key;
        this.value = value;
        this.hash = hash;
        this.res = resolution;
    }

    final LeafNode<T, V> copy() {
        return new LeafNode<>(key, value, hash, res);
    }

    final TombstoneNode<T, V> copyTombed() {
        return new TombstoneNode<>(key, value, hash, res);
    }

    final LeafNode<T, V> copyUntombed() {
        return new LeafNode<>(key, value, hash, res);
    }

    final public Map.Entry<H3CellId<T>, V> kvPair() {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    @Override
    public int resolution() {
        return res;
    }

    @Override
    protected final int cachedSize(SpatialConcurrentTrieMap<T, V> instance) {
        return 1;
    }
}
