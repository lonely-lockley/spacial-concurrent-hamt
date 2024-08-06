package com.github.lonelylockley.spacial.ctrie.nodes;

import com.github.lonelylockley.spacial.ctrie.SpacialConcurrentTrieMap;
import com.github.lonelylockley.spacial.ctrie.H3CellId;

import java.util.AbstractMap;
import java.util.Map;

public class TombstoneNode<T, V> extends TerminalNode<T, V> {

    protected final H3CellId<T> key;
    protected final V value;
    protected final long hash;
    protected final int res;

    TombstoneNode(final H3CellId<T> key, final V value, final long hash, final int res) {
        this.key = key;
        this.value = value;
        this.hash = hash;
        this.res = res;
    }

    final TombstoneNode<T, V> copy () {
        return new TombstoneNode<>(key, value, hash, res);
    }

    final TombstoneNode<T, V> copyTombed () {
        return new TombstoneNode<>(key, value, hash, res);
    }

    final LeafNode<T, V> copyUntombed () {
        return new LeafNode<>(key, value, hash, res);
    }

    final public Map.Entry<H3CellId<T>, V> kvPair () {
        return new AbstractMap.SimpleImmutableEntry<>(key, value);
    }

    @Override
    final public int cachedSize (SpacialConcurrentTrieMap<T, V> instance) {
        return 1;
    }

    @Override
    public int resolution() {
        return res;
    }
}
