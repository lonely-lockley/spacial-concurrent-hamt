package com.github.lonelylockley.spatial.ctrie;

import com.github.lonelylockley.spatial.ctrie.nodes.BaseNode;
import com.github.lonelylockley.spatial.ctrie.nodes.NodeWrapper;

class RDCSSDescriptor <T, V> extends BaseNode<T, V> {
    public final NodeWrapper<T, V> old;
    public final BaseNode<T, V> expectedmain;
    public final NodeWrapper<T, V> nv;
    private volatile boolean committed = false;

    public RDCSSDescriptor(final NodeWrapper<T, V> old, final BaseNode<T, V> expectedmain, final NodeWrapper<T, V> nv) {
        this.old = old;
        this.expectedmain = expectedmain;
        this.nv = nv;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

    public boolean isCommitted() {
        return committed;
    }

    @Override
    protected int cachedSize(SpatialConcurrentTrieMap<T, V> instance) {
        throw new RuntimeException("Not implemented");
    }

    @Override
    public int resolution() {
        throw new RuntimeException("Not implemented");
    }
}
