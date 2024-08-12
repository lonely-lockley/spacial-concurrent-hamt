package com.github.lonelylockley.spatial.ctrie.nodes;

import com.github.lonelylockley.spatial.ctrie.SpatialConcurrentTrieMap;

public class FailedNode<T, V> extends BaseNode<T, V> {

    private final BaseNode<T, V> failedOn;

    public FailedNode(BaseNode<T, V> failedOn) {
        this.failedOn = failedOn;
        setParent(failedOn);
    }

    @Override
    public int resolution() {
        return failedOn.resolution();
    }

    @Override
    protected int cachedSize(SpatialConcurrentTrieMap<T, V> instance) {
        return 0;
    }
}
