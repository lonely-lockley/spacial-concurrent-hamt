package com.github.lonelylockley.spatial.ctrie.nodes;

import com.github.lonelylockley.spatial.ctrie.SpatialConcurrentTrieMap;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

public abstract class BaseNode<T, V> implements Node<T, V> {

    private static final AtomicReferenceFieldUpdater<BaseNode, BaseNode> updater = AtomicReferenceFieldUpdater.newUpdater (BaseNode.class, BaseNode.class, "parent");
    private static final AtomicIntegerFieldUpdater<BaseNode> UPDATER = AtomicIntegerFieldUpdater.newUpdater(BaseNode.class, "size");

    protected volatile BaseNode<T, V> parent = null;
    private volatile int size = -1;

    protected boolean swapParent(BaseNode<T, V> oldValue, BaseNode<T, V> newValue) {
        return updater.compareAndSet(this, oldValue, newValue);
    }

    protected void setParent(BaseNode<T, V> newValue) {
        updater.set(this, newValue);
    }

    public BaseNode<T, V> getParent() {
        return updater.get(this);
    }

    public boolean swapSize(int oldval, int nval) {
        return UPDATER.compareAndSet(this, oldval, nval);
    }

    public void getSize(int nval) {
        UPDATER.set(this, nval);
    }

    public int getSize() {
        return size;
    }

    protected abstract int cachedSize (SpatialConcurrentTrieMap<T, V> instance);

}
