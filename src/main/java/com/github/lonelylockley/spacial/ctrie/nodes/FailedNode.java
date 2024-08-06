package com.github.lonelylockley.spacial.ctrie.nodes;

import com.github.lonelylockley.spacial.ctrie.SpacialConcurrentTrieMap;

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
    protected int cachedSize(SpacialConcurrentTrieMap<T, V> instance) {
        return 0;
    }
}
