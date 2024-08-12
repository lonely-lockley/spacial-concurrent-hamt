package com.github.lonelylockley.spatial.ctrie;

import java.util.Map;

public class TrieMapReadOnlyIterator<T, V> extends TrieMapIterator<T, V> {
    TrieMapReadOnlyIterator (int level, final SpatialConcurrentTrieMap<T, V> ct, boolean mustInit) {
        super (level, ct, mustInit);
    }

    TrieMapReadOnlyIterator (int level, SpatialConcurrentTrieMap<T, V> ct) {
        this (level, ct, true);
    }
    void initialize () {
        assert (ct.isReadOnly ());
        super.initialize ();
    }

    public void remove () {
        throw new UnsupportedOperationException ("Operation not supported for read-only iterators");
    }

    Map.Entry<H3CellId<T>, V> nextEntry(final Map.Entry<H3CellId<T>, V> rr) {
        // Return non-updatable entry
        return rr;
    }
}
